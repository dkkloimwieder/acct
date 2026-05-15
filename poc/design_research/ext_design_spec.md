# Unified Costing Ledger Extension — Design Specification

**Status:** Design spec, reference architecture
**Target:** PostgreSQL 18+, pgrx 0.17+
**Audience:** implementation team and future maintainers

---

## 0. Scope and goals

A pgrx-based PostgreSQL extension that records ledger transactions (inventory movements and balance changes) and computes their applied cost under arbitrary costing methods. The extension is the sole write path for the cost tables; all callers go through its SQL surface.

**Goals, in order of priority:**

1. **Correctness.** No inconsistency between the cost tables and the posting ledger that cannot be reconciled by replaying durable events. No silently-wrong applied costs.
2. **Throughput.** Maximize per-second op rate under bursty, mixed-item workloads, on a single primary.
3. **Operational simplicity.** Observability via `pg_stat_statements` and a small set of extension-exposed metrics. No external dependencies.
4. **Generality.** One apply path serving FIFO, weighted-average, standard, periodic, and specific-identification costing methods, plus a balance-only path for transactions with no costing component.

**Non-goals:**

- Multi-primary write coordination.
- Replica-side extension presence (replicas serve table reads only; the extension does not load on standbys).
- Direct application access to the underlying tables (writes go through the extension; direct INSERT/UPDATE on cost tables is revoked).

**Hard constraints:**

- Single primary. Streaming replicas exist but the extension does not run on them.
- All ledger writes route through the extension's SQL interface.
- The extension's design must not assume the user transaction owns nothing else; user transactions may include arbitrary non-ledger SQL.

---

## 1. System overview

### 1.1 The five architectural pieces

```
┌─────────────────────────────────────────────────────────────┐
│  User backend: BEGIN; SELECT ledger_apply(...); ...; COMMIT │
└────────────────────────┬────────────────────────────────────┘
                         │ SQL call
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Apply interface (pgrx #[pg_extern])                        │
│  - Resolves costing method per item                         │
│  - Pushes request to per-item queue                         │
│  - Waits on result slot                                     │
└────────────────────────┬────────────────────────────────────┘
                         │ shmem queue push
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Committer (elected from waiting backends)                  │
│  - Drains queue batch                                       │
│  - Calls CostingMethod::plan_batch()                        │
│  - Opens committer transaction, INSERTs rows, commits       │
│  - Writes results to slots, wakes waiters                   │
└────────────────────────┬────────────────────────────────────┘
                         │ INSERT into cost tables (committer tx)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Append-only tables                                         │
│  - cost_layers      (immutable layer events)                │
│  - cost_consumptions (immutable consumption events)         │
│  - cost_compensations (reversals for aborted user txs)      │
└─────────────────────────────────────────────────────────────┘
                         ▲
                         │ optional read-side cache update
┌────────────────────────┴────────────────────────────────────┐
│  Shmem layer cache (per-item, optimization only)            │
│  - Updated post-commit by the committer                     │
│  - Read by costing methods to avoid SPI on every plan       │
│  - Authoritative truth lives in tables                      │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 Core design principles

**Append-only data model.** No row in any cost table is ever updated. Reversals, adjustments, and corrections create new rows. The "current state" of any item is derived from its row history.

**Queue-mediated single-writer per item.** Concurrent calls for the same item are serialized through a per-item shmem queue. One backend at a time is the "committer" for an item, batching requests and flushing them in a single transaction. This eliminates concurrent SSI conflicts on the same item by construction.

**Committer transaction is independent of caller transaction.** The committer's INSERTs commit before the caller's transaction commits. Under compensation semantics (the lead design), if the caller's transaction aborts, an `XactCallback` enqueues a compensating reversal that the committer flushes on a subsequent tick. Under reservation semantics (the alternative), the committer holds capacity in shmem and only durably flushes on caller commit.

**Costing-method abstraction.** A `CostingMethod` trait encapsulates the per-method logic. The apply path, queue, and committer are method-agnostic. New methods are new trait implementations.

**Tables are the ultimate truth; shmem is a cache.** Any shmem state can be rebuilt by replaying the tables. Crash recovery rebuilds shmem from tables on extension load.

---

## 2. Data model

### 2.1 Schema

All tables append-only. No UPDATEs after initial INSERT. No DELETEs except controlled archival.

```sql
-- A cost layer event. A new row is created when inventory is added
-- (receipt), reversed (reversal), or adjusted (adjustment). Layers
-- belonging to the same conceptual unit of inventory share a layer_group_id.
CREATE TABLE cost_layers (
    layer_id        BIGSERIAL PRIMARY KEY,
    layer_group_id  BIGINT NOT NULL,
    item_id         BIGINT NOT NULL,
    qty             BIGINT NOT NULL,    -- signed; negative for reversals
    unit_cost       BIGINT,             -- NULL for methods that don't track per-layer cost
    born_at         TIMESTAMPTZ NOT NULL,
    born_seq        BIGINT NOT NULL,    -- monotone tiebreaker for same-instant births
    source_kind     TEXT NOT NULL CHECK (source_kind IN
                        ('receipt','reversal','adjustment','opening_balance',
                         'period_close','merge','split','synthetic')),
    source_ref      BIGINT,             -- FK to originating event in external system
    method_at_birth TEXT NOT NULL,      -- method that created this layer
    metadata        JSONB
);
CREATE INDEX cost_layers_item_born ON cost_layers (item_id, born_at, born_seq);
CREATE INDEX cost_layers_group     ON cost_layers (layer_group_id);
CREATE INDEX cost_layers_source    ON cost_layers (source_kind, source_ref)
    WHERE source_ref IS NOT NULL;

-- A consumption event. A new row is created when an issue consumes
-- from inventory. layer_id is NULL for methods that don't attribute
-- consumption to specific layers (e.g., weighted average, standard).
CREATE TABLE cost_consumptions (
    consumption_id    BIGSERIAL PRIMARY KEY,
    item_id           BIGINT NOT NULL,
    layer_id          BIGINT REFERENCES cost_layers(layer_id),
    qty               BIGINT NOT NULL CHECK (qty > 0),
    applied_unit_cost BIGINT,           -- NULL until set by method (periodic methods defer)
    consumed_at       TIMESTAMPTZ NOT NULL,
    consumed_seq      BIGINT NOT NULL,
    issue_id          BIGINT NOT NULL,
    method_used       TEXT NOT NULL,
    committer_tx_id   BIGINT NOT NULL,  -- the committer transaction that wrote this
    metadata          JSONB
);
CREATE INDEX cost_consumptions_item   ON cost_consumptions (item_id, consumed_at, consumed_seq);
CREATE INDEX cost_consumptions_layer  ON cost_consumptions (layer_id) WHERE layer_id IS NOT NULL;
CREATE INDEX cost_consumptions_issue  ON cost_consumptions (issue_id);

-- A compensation event. Created when a caller's transaction aborts
-- after the committer has durably flushed its consumption. The original
-- consumption row remains; this row offsets it for state derivation.
CREATE TABLE cost_compensations (
    compensation_id        BIGSERIAL PRIMARY KEY,
    compensates_consumption BIGINT NOT NULL REFERENCES cost_consumptions(consumption_id),
    compensated_at         TIMESTAMPTZ NOT NULL,
    compensated_seq        BIGINT NOT NULL,
    reason                 TEXT NOT NULL CHECK (reason IN
                              ('user_tx_abort','manual_correction','recon_fix')),
    metadata               JSONB
);
CREATE INDEX cost_compensations_target ON cost_compensations (compensates_consumption);

-- Method assignment per item over time. Items can change methods at
-- period boundaries; historical consumptions retain their method_used.
CREATE TABLE costing_method_assignments (
    item_id        BIGINT NOT NULL,
    method_id      TEXT NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to   TIMESTAMPTZ,
    config         JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (item_id, effective_from),
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);

-- Reservation table (used only under reservation semantics).
-- Holds in-flight capacity claims that haven't yet been durably flushed.
CREATE TABLE cost_reservations (
    reservation_id   BIGSERIAL PRIMARY KEY,
    item_id          BIGINT NOT NULL,
    layer_id         BIGINT REFERENCES cost_layers(layer_id),
    qty              BIGINT NOT NULL,
    reserved_at      TIMESTAMPTZ NOT NULL,
    backend_pid      INT NOT NULL,
    user_tx_xid      BIGINT NOT NULL,
    state            TEXT NOT NULL CHECK (state IN ('held','confirmed','released'))
);
CREATE INDEX cost_reservations_held ON cost_reservations (item_id, state) WHERE state = 'held';
```

### 2.2 Derived state

The "effective quantity remaining" in a layer group as of time T:

```sql
CREATE FUNCTION effective_qty(p_layer_group_id BIGINT, p_at TIMESTAMPTZ)
RETURNS BIGINT
LANGUAGE sql STABLE AS $$
    SELECT
        COALESCE(SUM(l.qty), 0)
      - COALESCE((
          SELECT SUM(c.qty)
          FROM cost_consumptions c
          WHERE c.layer_id IN (
              SELECT layer_id FROM cost_layers
              WHERE layer_group_id = p_layer_group_id
                AND born_at <= p_at
          )
          AND c.consumed_at <= p_at
          AND NOT EXISTS (
              SELECT 1 FROM cost_compensations cc
              WHERE cc.compensates_consumption = c.consumption_id
                AND cc.compensated_at <= p_at
          )
      ), 0)
    FROM cost_layers l
    WHERE l.layer_group_id = p_layer_group_id
      AND l.born_at <= p_at;
$$;
```

The current FIFO layer list for an item:

```sql
CREATE VIEW cost_layer_state AS
SELECT
    item_id,
    layer_group_id,
    MIN(born_at) AS oldest_birth,
    SUM(qty)
      - COALESCE((
          SELECT SUM(c.qty)
          FROM cost_consumptions c
          LEFT JOIN cost_compensations cc
                 ON cc.compensates_consumption = c.consumption_id
          WHERE c.layer_id IN (SELECT layer_id FROM cost_layers WHERE layer_group_id = l.layer_group_id)
            AND cc.compensation_id IS NULL
      ), 0) AS effective_qty,
    MIN(unit_cost) AS unit_cost  -- methods should assert consistent within group
FROM cost_layers l
GROUP BY item_id, layer_group_id
HAVING effective_qty > 0;
```

### 2.3 Invariants

These must hold at any committed snapshot:

| ID | Invariant |
|----|-----------|
| I1 | For every `cost_consumptions` row with non-NULL `layer_id`, the referenced layer group's effective_qty (computed without this consumption) ≥ the consumption's qty. |
| I2 | `method_used` on a consumption equals the method active for the item at `consumed_at` per `costing_method_assignments`. |
| I3 | Every `cost_consumptions` row has a corresponding posting_lines row in the application schema *unless* it has a `cost_compensations` row of reason='user_tx_abort'. |
| I4 | `consumed_seq` is monotone within `(item_id, consumed_at)`. |
| I5 | `born_seq` is monotone within `(item_id, born_at)`. |
| I6 | A `cost_compensations` row of reason='user_tx_abort' exists for exactly those consumptions whose committer-tx-id wrote rows that no posting_lines row references. |
| I7 | Under reservation semantics: every 'held' reservation either has a corresponding committed consumption (state→'confirmed') or has expired and is released (state→'released'). |
| I8 | No two `cost_consumptions` rows share a `consumption_id`; sequences are globally unique. |

Invariants I3 and I6 are application-layer; the extension can only verify these by joining against an external posting_lines table whose schema it doesn't own. The extension exposes a recon function for this.

### 2.4 Retention and archival

Append-only growth requires periodic archival. Eligible for archival:

- Layer groups where `effective_qty = 0` and all member layers' `born_at < retention_threshold`.
- All consumptions referencing archivable layers.
- All compensations referencing archivable consumptions.

Archival policy (configurable):

- A bgworker scans for archivable layer groups on a slow cadence (default daily).
- Eligible rows are moved to `cost_layers_archive`, `cost_consumptions_archive`, `cost_compensations_archive`.
- Archive tables are identical schema; can be moved to cold storage / partitioned away / TRUNCATEd by policy.
- Reads needing historical state union live and archive tables; standard recon does not.

---

## 3. The costing method trait

### 3.1 Interface

```rust
pub trait CostingMethod: Send + Sync {
    fn method_id(&self) -> &'static str;

    /// Plan the cost computation for a batch of requests against
    /// a snapshot of the item's history. Returns the rows to INSERT
    /// and the applied_cost to return to each caller.
    ///
    /// MUST be deterministic: same input → same output.
    /// MUST be pure: no SPI, no shmem mutation, no side effects.
    /// MUST handle the empty batch case (return empty result).
    fn plan_batch(
        &self,
        batch: &ConsumeBatch,
        snapshot: &ItemSnapshot,
    ) -> PlanResult;

    /// Handle a new layer-creating event (receipt, reversal, adjustment).
    /// Returns the cost_layers rows to INSERT.
    fn plan_layer_event(
        &self,
        event: &LayerEvent,
        snapshot: &ItemSnapshot,
    ) -> LayerPlan;

    /// Method-specific invariants checked by the constraint trigger
    /// at commit. Returns Ok or a description of the violation.
    /// MUST be deterministic and pure.
    fn validate_invariants(
        &self,
        affected_layer_groups: &[i64],
        snapshot: &ItemSnapshot,
    ) -> Result<(), InvariantViolation>;
}

pub struct ConsumeBatch {
    pub item_id: i64,
    pub requests: Vec<ConsumeRequest>,
}

pub struct ConsumeRequest {
    pub request_seq: u64,
    pub qty: i64,
    pub consumed_at: Timestamp,
    pub issue_id: i64,
    pub config_override: Option<JsonValue>,
}

pub struct ItemSnapshot {
    pub item_id: i64,
    pub layers: Vec<LayerView>,           // live layers, FIFO order
    pub standard_cost: Option<i64>,       // for standard method
    pub period_state: Option<PeriodState>, // for periodic methods
    pub method_config: JsonValue,
}

pub struct LayerView {
    pub layer_group_id: i64,
    pub layer_id: i64,
    pub unit_cost: Option<i64>,
    pub effective_qty: i64,
    pub born_at: Timestamp,
}

pub struct PlanResult {
    pub consumption_rows: Vec<ConsumptionRow>,
    pub layer_rows: Vec<LayerRow>,    // some methods emit auxiliary layers (e.g., merge layers)
    pub results: Vec<RequestResult>,  // one per ConsumeRequest, in input order
}

pub struct RequestResult {
    pub request_seq: u64,
    pub applied_unit_cost: i64,
    pub applied_total_cost: i64,
    pub consumption_ids: Vec<i64>,   // assigned at INSERT time; populated post-commit
}
```

### 3.2 Method registry and dispatch

```rust
pub struct MethodRegistry {
    methods: HashMap<String, Box<dyn CostingMethod>>,
}

impl MethodRegistry {
    pub fn new() -> Self {
        let mut r = Self { methods: HashMap::new() };
        r.register(Box::new(FifoMethod::default()));
        r.register(Box::new(WeightedAverageMethod::default()));
        r.register(Box::new(StandardCostMethod::default()));
        r.register(Box::new(SpecificIdMethod::default()));
        r.register(Box::new(PeriodicLifoMethod::default()));
        r.register(Box::new(PeriodicWeightedAverageMethod::default()));
        r.register(Box::new(BalanceOnlyMethod::default()));  // for items/txs with no costing component
        r
    }

    pub fn resolve(&self, item_id: i64, at: Timestamp) -> &dyn CostingMethod {
        let assignment = lookup_assignment(item_id, at);
        self.methods.get(&assignment.method_id)
            .expect("unknown method").as_ref()
    }
}

fn lookup_assignment(item_id: i64, at: Timestamp) -> AssignmentRow {
    // Hot path: backend-local cache keyed by (item_id, at-bucket).
    // Cold path: SPI into costing_method_assignments.
    // Cache invalidated by NOTIFY on method assignment changes.
    METHOD_ASSIGNMENT_CACHE.with(|c| {
        c.borrow_mut().get_or_load(item_id, at)
    })
}
```

### 3.3 FIFO method

```rust
pub struct FifoMethod;

impl CostingMethod for FifoMethod {
    fn method_id(&self) -> &'static str { "fifo" }

    fn plan_batch(&self, batch: &ConsumeBatch, snap: &ItemSnapshot) -> PlanResult {
        let mut layers = snap.layers.clone();  // local mutable copy
        let mut consumption_rows = Vec::new();
        let mut results = Vec::with_capacity(batch.requests.len());

        // Sort requests by consumed_at, consumed_seq for deterministic order.
        let sorted = sort_requests(&batch.requests);

        for req in &sorted {
            let mut remaining = req.qty;
            let mut total_cost: i64 = 0;
            let mut cons_ids = Vec::new();

            for layer in layers.iter_mut() {
                if remaining == 0 { break; }
                if layer.effective_qty <= 0 { continue; }
                let take = remaining.min(layer.effective_qty);
                let unit = layer.unit_cost.expect("FIFO requires per-layer unit_cost");
                total_cost = total_cost.saturating_add(take.saturating_mul(unit));
                layer.effective_qty -= take;
                remaining -= take;
                consumption_rows.push(ConsumptionRow {
                    item_id: batch.item_id,
                    layer_id: Some(layer.layer_id),
                    qty: take,
                    applied_unit_cost: Some(unit),
                    consumed_at: req.consumed_at,
                    issue_id: req.issue_id,
                    method_used: "fifo".into(),
                });
                cons_ids.push(0);  // backfilled after INSERT
            }

            if remaining > 0 {
                return PlanResult::error(
                    InvariantViolation::InsufficientInventory {
                        item: batch.item_id, want: req.qty, short: remaining,
                    });
            }

            let applied_unit_cost = if req.qty > 0 { total_cost / req.qty } else { 0 };
            results.push(RequestResult {
                request_seq: req.request_seq,
                applied_unit_cost,
                applied_total_cost: total_cost,
                consumption_ids: cons_ids,
            });
        }

        PlanResult { consumption_rows, layer_rows: vec![], results }
    }

    fn plan_layer_event(&self, event: &LayerEvent, _snap: &ItemSnapshot) -> LayerPlan {
        // One layer row per event. Group ID typically = event.source_ref for receipts,
        // or = the layer group being adjusted for reversals/adjustments.
        LayerPlan {
            rows: vec![LayerRow {
                layer_group_id: event.group_id_or_new(),
                item_id: event.item_id,
                qty: event.qty,  // signed
                unit_cost: event.unit_cost,
                born_at: event.at,
                source_kind: event.kind,
                source_ref: event.source_ref,
                method_at_birth: "fifo".into(),
                metadata: event.metadata.clone(),
            }],
        }
    }

    fn validate_invariants(
        &self, affected: &[i64], snap: &ItemSnapshot,
    ) -> Result<(), InvariantViolation> {
        // Checked by deferred constraint trigger at commit:
        // every affected layer group's effective_qty ≥ 0.
        for group_id in affected {
            let eff = snap.layer_group_effective_qty(*group_id);
            if eff < 0 {
                return Err(InvariantViolation::LayerOverConsumed {
                    layer_group_id: *group_id, deficit: -eff,
                });
            }
        }
        Ok(())
    }
}
```

### 3.4 Weighted average method

```rust
pub struct WeightedAverageMethod;

impl CostingMethod for WeightedAverageMethod {
    fn method_id(&self) -> &'static str { "avg" }

    fn plan_batch(&self, batch: &ConsumeBatch, snap: &ItemSnapshot) -> PlanResult {
        // Average cost across all live layers as of the snapshot.
        let total_qty: i64 = snap.layers.iter().map(|l| l.effective_qty).sum();
        if total_qty <= 0 {
            return PlanResult::error(InvariantViolation::InsufficientInventory {
                item: batch.item_id, want: 1, short: 1,
            });
        }
        let total_value: i64 = snap.layers.iter()
            .map(|l| l.effective_qty.saturating_mul(l.unit_cost.unwrap_or(0)))
            .sum();
        let avg_unit_cost = total_value / total_qty;

        let mut remaining_qty = total_qty;
        let mut consumption_rows = Vec::new();
        let mut results = Vec::with_capacity(batch.requests.len());

        for req in &batch.requests {
            if req.qty > remaining_qty {
                return PlanResult::error(InvariantViolation::InsufficientInventory {
                    item: batch.item_id, want: req.qty, short: req.qty - remaining_qty,
                });
            }
            // One consumption row per request; layer_id = NULL.
            consumption_rows.push(ConsumptionRow {
                item_id: batch.item_id,
                layer_id: None,
                qty: req.qty,
                applied_unit_cost: Some(avg_unit_cost),
                consumed_at: req.consumed_at,
                issue_id: req.issue_id,
                method_used: "avg".into(),
            });
            results.push(RequestResult {
                request_seq: req.request_seq,
                applied_unit_cost: avg_unit_cost,
                applied_total_cost: avg_unit_cost.saturating_mul(req.qty),
                consumption_ids: vec![0],
            });
            remaining_qty -= req.qty;
        }

        PlanResult { consumption_rows, layer_rows: vec![], results }
    }

    fn plan_layer_event(&self, event: &LayerEvent, _snap: &ItemSnapshot) -> LayerPlan {
        // Average method still records layer events for traceability and
        // to maintain the running average. New receipt or reversal updates
        // the implicit average via the snapshot computation above.
        LayerPlan { rows: vec![layer_from_event(event, "avg")] }
    }

    fn validate_invariants(
        &self, _affected: &[i64], snap: &ItemSnapshot,
    ) -> Result<(), InvariantViolation> {
        // Average method's invariant: total effective qty across all layers
        // must be ≥ 0 after applying all consumptions in this commit.
        let total: i64 = snap.layers.iter().map(|l| l.effective_qty).sum();
        if total < 0 {
            return Err(InvariantViolation::ItemOverConsumed {
                item: snap.item_id, deficit: -total,
            });
        }
        Ok(())
    }
}
```

### 3.5 Standard cost method

```rust
pub struct StandardCostMethod;

impl CostingMethod for StandardCostMethod {
    fn method_id(&self) -> &'static str { "std" }

    fn plan_batch(&self, batch: &ConsumeBatch, snap: &ItemSnapshot) -> PlanResult {
        let std_cost = snap.standard_cost
            .ok_or(InvariantViolation::StandardCostNotSet { item: batch.item_id });
        let std_cost = match std_cost {
            Ok(c) => c,
            Err(e) => return PlanResult::error(e),
        };

        let mut consumption_rows = Vec::new();
        let mut results = Vec::with_capacity(batch.requests.len());

        for req in &batch.requests {
            // Standard cost doesn't track per-layer inventory, but we still
            // emit a consumption row for the audit trail. layer_id = NULL.
            consumption_rows.push(ConsumptionRow {
                item_id: batch.item_id,
                layer_id: None,
                qty: req.qty,
                applied_unit_cost: Some(std_cost),
                consumed_at: req.consumed_at,
                issue_id: req.issue_id,
                method_used: "std".into(),
            });
            results.push(RequestResult {
                request_seq: req.request_seq,
                applied_unit_cost: std_cost,
                applied_total_cost: std_cost.saturating_mul(req.qty),
                consumption_ids: vec![0],
            });
        }

        // Variance recording: standard cost doesn't track actuals during apply.
        // A separate period-close job reconciles actual receipts against
        // standard-cost consumptions and books variances to a GL account.
        PlanResult { consumption_rows, layer_rows: vec![], results }
    }

    fn plan_layer_event(&self, event: &LayerEvent, _snap: &ItemSnapshot) -> LayerPlan {
        // Standard cost still records layer events for traceability, but they
        // don't affect cost computation (which uses standard_cost lookup).
        LayerPlan { rows: vec![layer_from_event(event, "std")] }
    }

    fn validate_invariants(
        &self, _affected: &[i64], _snap: &ItemSnapshot,
    ) -> Result<(), InvariantViolation> {
        // Standard cost has no inter-row invariant. Negative inventory is
        // allowed (variance is recorded externally).
        Ok(())
    }
}
```

### 3.6 Specific identification method

```rust
pub struct SpecificIdMethod;

impl CostingMethod for SpecificIdMethod {
    fn method_id(&self) -> &'static str { "specific" }

    fn plan_batch(&self, batch: &ConsumeBatch, snap: &ItemSnapshot) -> PlanResult {
        // Each request must include a target layer_id in config_override.
        let mut layers = snap.layers.clone();
        let mut consumption_rows = Vec::new();
        let mut results = Vec::with_capacity(batch.requests.len());

        for req in &batch.requests {
            let target_id: i64 = req.config_override.as_ref()
                .and_then(|c| c.get("layer_id"))
                .and_then(|v| v.as_i64())
                .ok_or(InvariantViolation::SpecificIdMissingTarget)?;
            let layer = layers.iter_mut()
                .find(|l| l.layer_id == target_id)
                .ok_or(InvariantViolation::SpecificIdLayerNotFound { layer_id: target_id })?;
            if layer.effective_qty < req.qty {
                return PlanResult::error(InvariantViolation::SpecificIdInsufficient {
                    layer_id: target_id, want: req.qty, have: layer.effective_qty,
                });
            }
            let unit = layer.unit_cost.unwrap_or(0);
            layer.effective_qty -= req.qty;
            consumption_rows.push(ConsumptionRow {
                item_id: batch.item_id,
                layer_id: Some(target_id),
                qty: req.qty,
                applied_unit_cost: Some(unit),
                consumed_at: req.consumed_at,
                issue_id: req.issue_id,
                method_used: "specific".into(),
            });
            results.push(RequestResult {
                request_seq: req.request_seq,
                applied_unit_cost: unit,
                applied_total_cost: unit.saturating_mul(req.qty),
                consumption_ids: vec![0],
            });
        }

        PlanResult { consumption_rows, layer_rows: vec![], results }
    }

    fn plan_layer_event(&self, event: &LayerEvent, _snap: &ItemSnapshot) -> LayerPlan {
        LayerPlan { rows: vec![layer_from_event(event, "specific")] }
    }

    fn validate_invariants(
        &self, affected: &[i64], snap: &ItemSnapshot,
    ) -> Result<(), InvariantViolation> {
        // Same as FIFO: no layer group can go negative.
        for group_id in affected {
            let eff = snap.layer_group_effective_qty(*group_id);
            if eff < 0 {
                return Err(InvariantViolation::LayerOverConsumed {
                    layer_group_id: *group_id, deficit: -eff,
                });
            }
        }
        Ok(())
    }
}
```

### 3.7 Periodic methods (LIFO, weighted-average-periodic)

Periodic methods defer cost computation to period close. During the period, consumption rows are written with `applied_unit_cost = NULL` (or a provisional value).

```rust
pub struct PeriodicLifoMethod;

impl CostingMethod for PeriodicLifoMethod {
    fn method_id(&self) -> &'static str { "periodic-lifo" }

    fn plan_batch(&self, batch: &ConsumeBatch, _snap: &ItemSnapshot) -> PlanResult {
        let mut consumption_rows = Vec::new();
        let mut results = Vec::with_capacity(batch.requests.len());

        for req in &batch.requests {
            // Provisional applied_unit_cost; finalized at period close.
            let provisional = 0;  // or a method-configured estimate
            consumption_rows.push(ConsumptionRow {
                item_id: batch.item_id,
                layer_id: None,
                qty: req.qty,
                applied_unit_cost: None,  // signaling deferred
                consumed_at: req.consumed_at,
                issue_id: req.issue_id,
                method_used: "periodic-lifo".into(),
            });
            results.push(RequestResult {
                request_seq: req.request_seq,
                applied_unit_cost: provisional,
                applied_total_cost: provisional.saturating_mul(req.qty),
                consumption_ids: vec![0],
            });
        }

        PlanResult { consumption_rows, layer_rows: vec![], results }
    }

    fn plan_layer_event(&self, event: &LayerEvent, _snap: &ItemSnapshot) -> LayerPlan {
        LayerPlan { rows: vec![layer_from_event(event, "periodic-lifo")] }
    }

    fn validate_invariants(
        &self, _affected: &[i64], _snap: &ItemSnapshot,
    ) -> Result<(), InvariantViolation> {
        Ok(())  // periodic methods enforce invariants at period close
    }
}

/// Extended trait for methods that have a periodic close step.
pub trait PeriodicCostingMethod: CostingMethod {
    fn close_period(
        &self,
        ctx: &PeriodCloseContext,
    ) -> PeriodCloseResult;
}

pub struct PeriodCloseContext {
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    pub items: Vec<i64>,
    pub snapshot: ItemSnapshot,
}

pub struct PeriodCloseResult {
    /// Updates to apply_unit_cost on existing consumption rows.
    /// These are the ONLY UPDATE operations the extension performs.
    pub consumption_finalizations: Vec<ConsumptionFinalization>,
    /// New layer rows representing period-close adjustments.
    pub layer_rows: Vec<LayerRow>,
    /// Variance entries for the GL.
    pub variances: Vec<VarianceRow>,
}
```

### 3.8 Balance-only method

The dispatch table must be total: every item assigned a costing method, every transaction routed through one. Some transactions, however, have no inventory-costing component:

- Balance transfers in the cash ledger (move $100 from account A to B). No item, no FIFO, no average — just a balance change recorded for audit and reconciliation.
- Items intentionally not costed: samples, marketing collateral, items whose cost is expensed at purchase rather than at consumption. Quantity is tracked; cost is zero by policy.
- Future categories where the apply pipeline is the right place to record a movement but no costing math applies.

`BalanceOnlyMethod` is the trivial method that satisfies this. It emits a consumption row with `applied_unit_cost = 0`, performs no layer interaction, and enforces no invariants. It exists so the dispatch table is total — no special-casing for "transactions without costing" in the apply path. The unified pipeline handles every transaction; the method that applies decides whether to do costing work.

This is **not** cash-basis accounting (which is a timing concept about when revenue and expense are recognized, orthogonal to how inventory is costed). This is just "this transaction has no costing dimension."

```rust
pub struct BalanceOnlyMethod;

impl CostingMethod for BalanceOnlyMethod {
    fn method_id(&self) -> &'static str { "balance-only" }

    fn plan_batch(&self, batch: &ConsumeBatch, _snap: &ItemSnapshot) -> PlanResult {
        let mut consumption_rows = Vec::new();
        let mut results = Vec::with_capacity(batch.requests.len());
        for req in &batch.requests {
            consumption_rows.push(ConsumptionRow {
                item_id: batch.item_id,
                layer_id: None,
                qty: req.qty,
                applied_unit_cost: Some(0),
                consumed_at: req.consumed_at,
                issue_id: req.issue_id,
                method_used: "balance-only".into(),
            });
            results.push(RequestResult {
                request_seq: req.request_seq,
                applied_unit_cost: 0,
                applied_total_cost: 0,
                consumption_ids: vec![0],
            });
        }
        PlanResult { consumption_rows, layer_rows: vec![], results }
    }

    fn plan_layer_event(&self, event: &LayerEvent, _snap: &ItemSnapshot) -> LayerPlan {
        // Balance-only items can still have "layer events" in the abstract
        // sense (e.g., an opening balance entry), recorded for audit but with
        // no effect on subsequent cost computation.
        LayerPlan { rows: vec![layer_from_event(event, "balance-only")] }
    }

    fn validate_invariants(
        &self, _affected: &[i64], _snap: &ItemSnapshot,
    ) -> Result<(), InvariantViolation> {
        Ok(())  // no inter-row invariants; balance-only is intentionally unconstrained
    }
}
```

The existing balance ledger (the `(balance, qty)` pair maintained per account in the current A2 architecture) folds into this method. Each balance transaction becomes one consumption row stamped `method_used = 'balance-only'`. The balance state itself is then a derivation: `SUM(consumption.qty) - SUM(compensation.qty)` per account, optionally cached in shmem for fast reads — exactly the same caching pattern as the inventory methods, just with simpler semantics.

### 3.9 Tradeoffs of the trait abstraction

**Pro:**
- Single apply path; methods are pluggable.
- Method-specific complexity isolated to the trait impl.
- Audit format uniform across methods (`method_used` stamped on every row).
- Mixed-method workloads work transparently.

**Con:**
- Virtual dispatch per `plan_batch` call (~10ns; negligible).
- Methods that don't need certain fields of `ItemSnapshot` still pay the SPI cost to populate them. Mitigation: lazy snapshot accessors that fault in fields on first access.
- Trait shape may evolve as new methods are added; expect minor breaking changes in early versions.
- Period-close trait is separate from the apply-time trait. Methods that have both must implement both. Acceptable; alternative (one mega-trait) is worse.

---

## 4. Concurrency architecture

### 4.1 Per-item queues

```rust
const QUEUE_SHARD_COUNT: usize = 256;  // power of two; sized at extension build
const REQUESTS_PER_SHARD: usize = 4096;
const SLOTS_PER_SHARD: usize = 4096;

#[repr(C, align(64))]
pub struct QueueShard {
    /// LWLock tranche entry; protects queue head/tail and committer election.
    pub lock_idx: u32,
    pub _pad0: [u8; 4],

    /// Ring buffer of pending requests.
    pub requests: [PendingRequest; REQUESTS_PER_SHARD],
    pub head: AtomicU32,
    pub tail: AtomicU32,

    /// Result slot pool (indexed by slot_id).
    pub slots: [ResultSlot; SLOTS_PER_SHARD],
    pub slot_alloc_next: AtomicU32,

    /// Committer election state.
    pub committer_pid: AtomicI32,    // 0 = no committer; otherwise backend pid
    pub committer_acquired_at: AtomicU64,  // monotonic ns; for lease timeout

    /// Per-shard sequence counter; monotone for request ordering.
    pub next_seq: AtomicU64,

    /// Items mapped to this shard. Used by the committer to subset
    /// the drain by item_id within the shard.
    /// (No state stored here; computed from request stream.)
}

#[repr(C)]
pub struct PendingRequest {
    pub valid: AtomicU8,                // 0=empty, 1=filled, 2=abandoned
    pub _pad: [u8; 7],
    pub request_seq: u64,
    pub item_id: i64,
    pub operation: RequestOperation,    // Consume | LayerEvent | Reserve | Confirm | Compensate
    pub qty: i64,
    pub at_micros: u64,
    pub issue_id: i64,
    pub backend_pid: i32,
    pub slot_id: u32,
    pub config_override: [u8; 256],     // inline JSON for small configs
    pub config_overflow: bool,           // if true, JSON in spillover area
}

#[repr(C, align(64))]
pub struct ResultSlot {
    pub state: AtomicU8,                // 0=free, 1=allocated, 2=filled, 3=abandoned
    pub _pad: [u8; 7],
    pub applied_unit_cost: AtomicI64,
    pub applied_total_cost: AtomicI64,
    pub consumption_ids: [AtomicI64; 32],  // up to 32 rows per request; spillover if more
    pub consumption_count: AtomicU16,
    pub error_code: AtomicU16,           // 0 = success
    pub error_message: [u8; 240],        // inline error text
}
```

Item-to-shard mapping: `shard_id = hash(item_id) & (QUEUE_SHARD_COUNT - 1)`. The committer for a shard processes ALL items in that shard during its tick, sorted by item_id, with per-item sub-batches.

### 4.2 Request lifecycle

```
1. Caller acquires LWLock on shard (brief).
2. Caller allocates a slot (atomic increment, with free-list fallback).
3. Caller stores PendingRequest into ring at queue tail; CAS tail forward.
4. Caller releases shard lock.
5. Caller attempts committer election:
   - CAS committer_pid 0 → my_pid
   - If success: caller IS committer. Proceeds to drain.
   - If fail: caller is waiter. Waits on slot.
6a. Committer path:
    - Wait briefly (tick window) for more requests to arrive.
    - Acquire shard lock.
    - Drain all valid requests; mark them in-progress.
    - Release shard lock.
    - Group drained requests by item_id.
    - For each item: resolve method, build ItemSnapshot, call plan_batch.
    - Open committer transaction (sub-tx using BeginInternalSubTransaction).
    - INSERT all consumption_rows and layer_rows.
    - COMMIT.
    - For each request: write applied_cost into slot, mark state=filled.
    - Wake waiters (PROCSIGNAL_NOTIFY_INTERRUPT).
    - CAS committer_pid back to 0.
6b. Waiter path:
    - Loop: ProcSemaphoreSleep with timeout.
    - On wake: check slot state.
    - If filled: read result, return.
    - If abandoned (committer died, see 4.5): re-enqueue request, retry.
    - On query cancel: mark slot abandoned, return error.
```

### 4.3 Committer election and the batch window

Two configurable parameters via GUC:

- `ledger.batch_window_us`: the committer waits up to this many microseconds after winning election before draining, allowing batch accumulation. Default 500μs.
- `ledger.batch_size_max`: the committer flushes immediately if this many requests are queued, regardless of window. Default 1024.
- `ledger.committer_lease_ms`: max time a committer holds the role. If exceeded, another backend can steal the role. Default 100ms.

The committer's wait is implemented as `WaitLatch(MyLatch, WL_TIMEOUT, batch_window_us / 1000, ...)` with the shard's `request_count` checked on wake. If a new request arrived, it's included in the batch.

### 4.4 Multi-item transactions

A user transaction touching items A and B issues two `ledger_apply` calls. Each call resolves to a shard (potentially different shards). Each call independently pushes-and-waits.

If A and B happen to hash to the same shard: both requests join the same shard's queue. The committer for that shard handles both. They're processed in arrival order within the same committer transaction. The user transaction's two `ledger_apply` calls return their results sequentially.

If A and B hash to different shards: the user backend serializes them (call 1 returns before call 2 starts) but each shard's committer runs independently. Different shard committers run in parallel.

**This is the key win: even within one user transaction, ledger work for different items can be batched with other users' work for the same items.** A user touching items 1 and 2 in sequence can have their item-1 work batched with another user's item-1 work, while a third backend's item-2 work is batched with this user's item-2 work. Aggregate throughput scales with shard count.

### 4.5 Committer failure handling

Three failure modes:

**(a) Committer transaction fails.** The committer's transaction tries to INSERT and PG raises (constraint violation, deadlock with an unrelated tx, OOM). The committer:
- Catches the error via `PgTryBuilder`.
- Rolls back the committer sub-transaction.
- Writes error state to all slots in the batch.
- Wakes waiters.
- Releases committer role.

Waiters see an error in their slot and return the error to their caller. The caller's transaction can decide to retry the call (which will land in a new batch).

**(b) Committer backend dies mid-batch.** Detected by lease timeout. After `committer_lease_ms`, another backend pushing to the same shard sees the stale lease (`committer_acquired_at` too old), CAS-acquires the role, and runs `recover_orphaned_batch`:
- Scans the queue for in-progress requests.
- For each, checks: did the dead committer's transaction commit before death? (Look up `committer_tx_id` in pg_xact via `pg_xact_status`.)
- If committed: results are durable in cost_consumptions; backfill the slot from the table.
- If aborted or unknown: re-queue the request as fresh.

**(c) Slot abandoned by waiter.** The waiter received a query cancel and marked its slot abandoned. The committer, when processing, sees `valid == 2` (abandoned) and:
- If reservation semantics: skips the request entirely (no INSERT).
- If compensation semantics: still INSERTs (the work is idempotent at the item level); a separate cleanup process will compensate any orphaned consumption.

The choice between these depends on whether the request is idempotent. For `ledger_apply` with a unique issue_id, processing-and-discarding is safe (the issue_id pins the work). For requests without a natural idempotency key, dropping is safer.

### 4.6 Backpressure

When a shard's ring buffer is full (`tail - head >= REQUESTS_PER_SHARD - 1`), `ledger_apply` blocks on a condition variable until the committer drains. The caller's `CHECK_FOR_INTERRUPTS` runs periodically so query cancel works.

A GUC `ledger.queue_full_timeout_ms` bounds the wait. On timeout, the call returns an error (`queue_full`), and the caller's transaction must decide whether to retry or abort.

### 4.7 Tradeoffs of queue+committer

**Pro:**
- Eliminates SSI conflicts on hot items by construction (no two concurrent transactions write the same item's cost rows).
- Throughput scales with shard count for disjoint workloads.
- Batching amortizes commit cost across many requests.
- Failure handling is local to the shard.

**Con:**
- Adds latency to every call (queue push + slot wait). Measured: ~10-50μs under no contention.
- Memory cost: `QUEUE_SHARD_COUNT × (size of QueueShard) ≈ 256 × 4MB = 1GB` of shmem. Tunable down for small deployments.
- Committer-in-user-backend means the committing backend's connection is busy. Other backends pushing to the same shard during the commit window are queued.
- Shard count must be chosen at extension build time; resharding requires extension restart.
- Cross-shard transactions don't get the batching benefit; each shard is independent.

**Potential issues:**
- **Lease timeout false-positive.** A committer doing slow work (e.g., a method calling SPI in plan_batch — which is forbidden but possible if a method violates the contract) might exceed the lease and have its work stolen. Mitigation: methods are tested for purity; CI enforces no-SPI in plan_batch.
- **Shard hotspot.** If hash(item_id) clusters multiple hot items into one shard, that shard's committer becomes a bottleneck while other shards idle. Mitigation: hash function is randomized; if production shows clustering, allow shard reassignment via item_id rehashing (requires extension restart).
- **Slot exhaustion.** If many backends abandon slots without the committer marking them free, slots leak. Mitigation: slot allocator has a free-list rebuilt on shard idle; abandoned slots are recovered.

---

## 5. Transaction lifecycle: compensation vs. reservation

Both semantics are documented. Compensation is the lead design.

### 5.1 Compensation semantics (default)

```
User tx:                            Extension:
BEGIN
SELECT ledger_apply(...)            → enqueue request to shard
                                    → (some backend is committer for shard)
                                    → committer batch tick fires
                                    → committer tx begins (committer sub-tx)
                                    →   INSERT INTO cost_consumptions ...
                                    →   COMMIT
                                    → result written to slot
                                    ← user backend returns applied_cost
INSERT INTO posting_lines ...
COMMIT                              (no extension involvement)
```

If user tx aborts after `ledger_apply` returns:

```
ROLLBACK                            (user tx aborts)
                                    → XactCallback ABORT fires in user backend
                                    → enqueue compensation request for each
                                      committer_tx_id this user backend received
                                    → compensation processed by next committer tick
                                    → INSERT INTO cost_compensations
                                      (compensates_consumption=N, reason='user_tx_abort')
```

`cost_compensations` rows offset the original consumption in all derivations (effective_qty, layer state, recon). The audit trail shows the original consumption AND its compensation, both with timestamps and tx attribution.

**Invariant I6 enforcement:** A bgworker periodically joins `cost_consumptions` against the application's `posting_lines` table; any consumption without a corresponding posting_line that's also not compensated triggers an alert.

**Tradeoffs:**

| Pro | Con |
|-----|-----|
| Pure append-only; no UPDATE/DELETE anywhere on the hot path | Audit shows reversed transactions; accounting team must understand this |
| User tx abort is non-blocking (just enqueues compensation) | Brief window where consumption is durable but unmatched in posting_lines |
| Compensation is just another request type; reuses queue/committer machinery | Recon requires joining against external posting_lines |
| If extension crashes mid-compensation, the next start re-processes the abort callback queue (persisted via separate table, see 5.3) | Higher row volume long-term (compensations accumulate) |

### 5.2 Reservation semantics (alternative)

```
User tx:                            Extension:
BEGIN
SELECT ledger_apply(...)            → enqueue Reserve request
                                    → committer processes Reserve:
                                    →   computes plan via method.plan_batch
                                    →   inserts cost_reservations rows (state='held')
                                    →   does NOT insert cost_consumptions yet
                                    → result written to slot
                                    ← user backend returns applied_cost
INSERT INTO posting_lines ...
COMMIT                              → XactCallback PRE_COMMIT fires
                                    → enqueue Confirm request
                                    → committer processes Confirm:
                                    →   INSERTs cost_consumptions from reservations
                                    →   updates cost_reservations state='confirmed'
                                    →   user backend COMMIT completes
                                    OR
ROLLBACK                            → XactCallback ABORT fires
                                    → enqueue Release request
                                    → committer processes Release:
                                    →   updates cost_reservations state='released'
                                    →   layer state in shmem is unchanged
                                      (reservation was never reflected in tables)
```

**Tradeoffs:**

| Pro | Con |
|-----|-----|
| No phantom-committed consumptions during user tx | Layer capacity calculations must account for held reservations |
| Audit trail has no reversal rows in steady state | UPDATE on `cost_reservations.state` violates append-only |
| Tighter consistency: cost is durable iff posting is durable | XactCallback PRE_COMMIT adds latency to user tx commit |
| | Held reservations across long user txs block other consumers |
| | Crash recovery complexity: held reservations from dead backends need GC |

### 5.3 Compensation queue durability

A subtle issue with compensation: the XactCallback ABORT runs in the user backend; if it enqueues a compensation that hasn't been processed when the user backend dies, the compensation is lost.

Mitigation: the ABORT callback INSERTs into a `pending_compensations` table within the aborting user transaction. Since the user tx is already aborting, this INSERT also rolls back — except we use `AUTONOMOUS_TRANSACTION` semantics via a dblink-equivalent or a writer bgworker that owns a separate connection. The simplest reliable mechanism:

```rust
fn enqueue_compensation_for_committer_tx(tx_id: BigInt) {
    // Push to shmem queue; the committer bgworker picks it up.
    // If the user backend dies before the committer processes, the
    // queued request is still in shmem. If the postmaster dies,
    // shmem is lost, but on restart, a startup recovery process
    // scans cost_consumptions for committer_tx_ids whose user_tx
    // (recorded in a side table) shows as aborted/unknown in pg_xact,
    // and emits the compensations from there.
}
```

The startup recovery scan is what makes this durable: shmem-queued compensations that didn't process pre-crash are recomputed from the pg_xact status of the user transactions.

### 5.4 Choosing between semantics

Default to compensation. Switch to reservation if:

- Auditors object to reversal rows in normal operation (they're rare under low abort rate, but visible).
- The window of "consumption durable but posting not" creates regulatory reporting problems.
- User transactions are very short (microseconds) so reservation overhead is negligible.

The extension supports both via a per-item-or-global GUC `ledger.semantics = 'compensation' | 'reservation'`. The committer dispatches accordingly. Both code paths are maintained; tests cover both.

---

## 6. Shmem layout

### 6.1 What lives in shmem

```rust
pub struct LedgerShmem {
    pub queue_shards: [QueueShard; QUEUE_SHARD_COUNT],
    pub layer_cache: LayerCache,          // optional read-side optimization
    pub global_state: GlobalState,
}

pub struct LayerCache {
    pub items: [ItemCacheEntry; ITEM_CACHE_SIZE],
    pub lookup_index: SmallHashMap<i64, u32>,  // item_id → index
}

#[repr(C, align(64))]
pub struct ItemCacheEntry {
    pub occupied: AtomicU8,
    pub _pad: [u8; 7],
    pub item_id: AtomicI64,
    pub last_updated_seq: AtomicU64,
    pub layer_count: AtomicU16,
    pub _pad2: [u8; 6],
    pub layers: [CachedLayer; MAX_CACHED_LAYERS_PER_ITEM],
    pub needs_reload: AtomicU8,           // set on suspected inconsistency
}

#[repr(C)]
pub struct CachedLayer {
    pub layer_group_id: i64,
    pub layer_id: i64,
    pub effective_qty: i64,
    pub unit_cost: i64,
    pub born_at_micros: u64,
}

pub struct GlobalState {
    pub apply_seq: AtomicU64,
    pub committer_tx_seq: AtomicU64,
    pub method_assignment_version: AtomicU64,  // bumped on assignment changes
    pub backpressure_count: AtomicU64,
    pub batch_size_total: AtomicU64,
    pub batch_count_total: AtomicU64,
}
```

### 6.2 Cache coherence

The cache is a hint, not authoritative. Three update points:

**Post-commit by committer.** After the committer's tx commits, it updates the cache for affected items in shmem. This is best-effort; if the cache is full (no slot for a new item), it's skipped.

**Lazy reload on read.** Methods reading the cache check `needs_reload` and `last_updated_seq` against `apply_seq`. If stale, the method falls back to SPI and refreshes the cache.

**Invalidation on method assignment change.** When `costing_method_assignments` changes for an item, a NOTIFY fires; bgworkers and committers invalidate cached entries for the affected item.

### 6.3 Cache sizing

- `ITEM_CACHE_SIZE`: 65536 entries by default. Adequate for ~10K items with headroom.
- `MAX_CACHED_LAYERS_PER_ITEM`: 64 layers. Items with more live layers fall back to SPI for all reads (or use a spillover cache; not in initial scope).

### 6.4 Why the cache is optional

The cache is purely a read-side optimization. With cache disabled (or empty), every `plan_batch` call does SPI to fetch live layers — correct but slower. The cache amortizes the SPI cost across requests touching the same item in a short window.

For initial implementation: ship with cache disabled, measure end-to-end performance, enable cache if profiling shows SPI in `plan_batch` is a bottleneck.

### 6.5 Tradeoffs of caching

**Pro:** Avoids SPI on hot items; can serve a plan_batch without touching tables when layer state is unchanged.

**Con:**
- Memory cost (4MB per 65536-entry cache with 64 layers each).
- Coherence complexity (must invalidate on method changes, post-commit updates).
- Can mask bugs: an incorrect cache entry produces incorrect plans without an obvious error signal. Mitigation: periodic recon job that cross-checks cache against tables.

---

## 7. Replication and HA

### 7.1 Primary-only extension

The extension is loaded only on the primary via `shared_preload_libraries`. Replicas do not load the extension. Replicas serve reads from the replicated tables only; they cannot serve writes (PG enforces this).

This eliminates the entire replica-coherence problem. Shmem on the primary is the only cache. Replicas read tables directly (slower, but correct).

### 7.2 Failover

When a replica is promoted:

1. The new primary loads the extension (via `shared_preload_libraries` in its config; same binary as the old primary).
2. `_PG_init` runs; shmem is allocated empty.
3. The committer bgworker(s) start.
4. The cache is empty; first applies for any item incur SPI to populate.
5. The compensation recovery job runs as part of `_PG_init`'s startup hook:
   - Scans for `committer_tx_id`s in `cost_consumptions` whose user-tx record shows as aborted in pg_xact.
   - Emits compensations for those.
   - This handles any in-flight aborts that didn't fire their XactCallback before crash.

### 7.3 Logical replication

Out of scope for initial design. Implications:
- Replicating `cost_layers` and `cost_consumptions` to another cluster works via standard logical replication.
- Replicating the extension's shmem state is not feasible; the target cluster rebuilds from tables.
- Order-sensitive consumers (e.g., a downstream system relying on consumed_seq ordering) need careful slot configuration.

### 7.4 Tradeoffs

**Pro:** Massively simpler than multi-primary; failover is standard PG streaming replication.

**Con:** Replicas can't serve writes; all ledger writes go to the primary. For a write-heavy system this is the primary's load ceiling.

---

## 8. Operational concerns

### 8.1 Observability

Exposed via `#[pg_extern]` functions returning current values:

```rust
#[pg_extern] fn ledger_apply_seq() -> i64;
#[pg_extern] fn ledger_committer_tx_seq() -> i64;
#[pg_extern] fn ledger_shard_stats() -> TableIterator<(/* per-shard metrics */)>;
#[pg_extern] fn ledger_method_stats() -> TableIterator<(/* per-method metrics */)>;
#[pg_extern] fn ledger_queue_depth() -> i64;
#[pg_extern] fn ledger_backpressure_count() -> i64;
#[pg_extern] fn ledger_committer_tx_failures() -> i64;
#[pg_extern] fn ledger_orphan_compensations() -> i64;
#[pg_extern] fn ledger_avg_batch_size() -> f64;
#[pg_extern] fn ledger_p99_latency_us() -> i64;  // requires latency histogram
```

These integrate with `pg_stat_statements` via standard SELECTs; operators wire them into Prometheus or equivalent at the application layer.

### 8.2 GUCs

```
ledger.batch_window_us          (int, 100-10000, default 500)
ledger.batch_size_max           (int, 16-65536, default 1024)
ledger.committer_lease_ms       (int, 10-10000, default 100)
ledger.queue_full_timeout_ms    (int, 100-60000, default 5000)
ledger.semantics                (enum: 'compensation' | 'reservation', default 'compensation')
ledger.cache_enabled            (bool, default false initially)
ledger.recon_interval_s         (int, 60-86400, default 3600)
ledger.archival_threshold_days  (int, 30-3650, default 90)
ledger.shard_count              (int, postmaster-only, default 256, power of two)
ledger.requests_per_shard       (int, postmaster-only, default 4096)
```

### 8.3 BGWorkers

Three bgworkers:

**Compensation recovery worker.** Started at extension load. Scans for aborted user transactions whose committer-tx work is undone. Runs on startup and on a slow cadence (5 min). Idempotent.

**Recon worker.** Periodic invariant verifier. Joins cost_consumptions against application posting_lines. Reports drift via metrics. Runs every `ledger.recon_interval_s`.

**Archival worker.** Moves fully-reconciled layer groups to archive tables. Runs daily by default.

No bgworker handles the apply hot path. All apply work is done by elected committers in user backends.

### 8.4 Migration from existing system

Out of scope detail, but the sketch:

1. Deploy new schema (cost_layers, cost_consumptions, etc.) alongside existing.
2. Backfill: for each existing inventory item, generate cost_layers rows from historical receipts; generate cost_consumptions rows from historical issues. Method assignments from existing item configs.
3. Run new extension in shadow mode: it processes the same calls as the old system, writing to new tables; results compared.
4. Cutover after shadow validation: switch the application to call the new SQL surface.
5. Old code paths archived after a retention period.

---

## 9. Testing methodology

### 9.1 Component-level tests

Each component gets its own test suite. All tests run via `cargo pgrx test` against the supported PG versions.

#### 9.1.1 Costing methods (pure function tests)

For each `impl CostingMethod`, test in isolation:

- **Determinism:** `plan_batch(B, S) == plan_batch(B, S)` for identical inputs.
- **Purity:** plan_batch with mocked snapshot makes no SPI calls (verified via custom SpiTracker that records all SPI access).
- **Boundary cases:**
  - Empty batch.
  - Single request batch.
  - Batch larger than typical (10000 requests).
  - Requests with `qty = 0`, `qty < 0` (should error).
  - Snapshot with zero layers.
  - Snapshot with negative effective_qty in a layer group.
  - Requests at the exact capacity of a layer.
  - Requests spanning multiple layers.
  - Requests exceeding total inventory.

For FIFO specifically:
- Strict FIFO order verification (oldest layer drained first).
- Multi-layer spans produce one consumption row per layer touched.
- Applied unit cost is the weighted average across spanned layers.

For weighted average:
- Average is computed across ALL layers including those with effective_qty=0 (treatment depends on method spec; for true moving average, only positive-qty layers count).
- Result is stable under non-consuming layer additions.

For standard cost:
- Result independent of layer state.
- Negative inventory is permitted.
- Variance recording (when implemented) matches expected formula.

#### 9.1.2 Queue mechanics

Tests using a controlled shard with mocked methods:

- **Push-pop ordering:** N requests pushed in order; committer drains in order.
- **Slot allocation:** allocating N slots; freeing in mixed order; verifying no slot leaks.
- **Backpressure:** filling a shard to capacity, asserting push blocks; draining one; asserting blocked push completes.
- **Committer election:**
  - Single backend pushes → wins election.
  - Two backends push simultaneously → one wins, other waits.
  - Committer releases → next push can win election.
- **Lease timeout:**
  - Committer holds role beyond lease without progress → another backend can steal.
  - Committer holds role within lease → no steal.
- **Wake correctness:**
  - Committer fills N slots and wakes N waiters → all waiters return.
  - Committer fills slots but one waiter is canceled mid-wait → canceled waiter doesn't deadlock others.

#### 9.1.3 Committer transaction handling

- **Successful commit:** consumption rows are durable; results are written to slots.
- **Transaction failure (constraint violation):** all slots receive error; no consumption rows are durable.
- **Transaction failure (deadlock):** retried up to N times; final failure surfaces to all slots.
- **Committer death mid-batch:** stale lease detected; recovery process correctly identifies committed vs. uncommitted committer transactions.

#### 9.1.4 Compensation path

- **Normal abort:** user tx aborts; compensation enqueued; committer processes; cost_compensations row exists.
- **Abort with no prior commit:** user tx aborts before any committer ran; no compensation needed.
- **Multiple aborts from same user tx:** N consume calls then abort → N compensations.
- **User backend death after commit:** committer-tx committed but user tx never aborted (backend died); compensation recovery worker detects via pg_xact and emits compensation.
- **Compensation enqueueing during shutdown:** extension shutdown drains compensation queue or persists undrained items.

#### 9.1.5 Reservation path

- **Normal flow:** reserve → confirm; consumption is durable, reservation is 'confirmed'.
- **Abort flow:** reserve → release; no consumption row; reservation is 'released'.
- **Reservation expiry:** held reservation past max age is released by GC worker.
- **Capacity accounting:** held reservations subtract from effective_qty in subsequent plans.
- **Confirm race:** two backends trying to confirm the same reservation → one succeeds, other gets error.

### 9.2 Multi-backend stress tests

Using a test harness that spawns N psql sessions and coordinates via barriers:

- **Concurrent disjoint:** N backends each touching distinct items → linear throughput scaling.
- **Concurrent same-shard:** N backends touching items that hash to one shard → throughput bounded by single committer; latency rises with N.
- **Concurrent same-item:** N backends consuming from item 42 → all serialized via one committer; verifying no SSI conflicts arise (because there are none — all consumption is serial within the committer).
- **Mixed read/write:** N writer backends + M reader backends (querying cost_consumptions); readers always see consistent state.
- **Long transactions:** A backend's user tx stays open for 60s while others apply; verifying compensation correctness on the long tx's eventual abort/commit.

### 9.3 Invariant verification

A property-based test (using `proptest` or similar) generates random sequences of:

- Layer events (receipts, reversals, adjustments)
- Consume requests
- User tx commits and aborts
- Backend deaths

After each step, all invariants I1-I8 are verified. Generated sequences run against:

- Each method in isolation.
- A mix of methods (different items use different methods).

### 9.4 Fault injection

Tests where specific failures are injected:

- **SPI failure during plan_batch:** the method shouldn't be calling SPI; injection verifies the contract.
- **WAL write failure during committer commit:** the committer tx aborts; slots receive error; no compensation needed (nothing was durable).
- **Backend SIGKILL during commit:** test framework kills the committer mid-commit. Verifies:
  - PG cleanup runs.
  - The committer-tx is either fully committed or fully aborted in pg_xact.
  - Recovery worker detects state and acts accordingly.
- **Shmem corruption:** simulated by writing garbage to a shmem region. Verifies extension detects and either recovers or fails safe (no silent data corruption).
- **Disk full during INSERT:** committer-tx fails cleanly; slots receive error.

### 9.5 Performance benchmarks

Reproducible benchmark suite measuring:

- **Throughput:** ops/sec under various workload shapes (uniform, hot-item, zipfian).
- **Latency:** p50/p99/p99.9 from `ledger_apply` call to return.
- **Batch size distribution:** average, p99 batch size under various queue depths.
- **Scaling:** throughput vs. concurrent backend count, from 1 to 256.
- **Method comparison:** same workload run through each method; relative cost of each.

Benchmarks are version-pinned; regressions of >5% on any metric fail CI.

### 9.6 Period-close tests

For periodic methods:

- Consumption rows during the period have `applied_unit_cost = NULL`.
- Period close correctly finalizes all of them.
- Variance recording matches expected formula.
- Period close is atomic: either all consumptions in the period are finalized or none are.
- Re-running period close on an already-closed period is a no-op (idempotency).

### 9.7 Integration with external posting_lines

The extension cannot fully test invariant I3 in isolation. The test harness provides a mock posting_lines table and:

- Verifies recon job correctly flags consumptions without matching posting_lines.
- Verifies recon job correctly ignores compensated consumptions.
- Verifies recon performance is acceptable for the production posting_lines volume.

### 9.8 Method assignment tests

- Item changes method mid-life: existing consumptions retain their `method_used`; new consumptions use the new method.
- Method assignment cache invalidation on NOTIFY.
- Effective-time semantics: `consumed_at` determines which method applies, not current wall-clock time.

---

## 10. Open issues

Marked clearly so they're not lost:

**[OPEN-1] Cross-shard transaction atomicity.** A user tx touching items in shards A and B has two independent committer transactions. If shard A's committer succeeds and shard B's fails, the user tx might see inconsistent results. Current design: each shard reports its own result; the caller's tx commits or aborts as a whole; compensation handles partial state. This is acceptable under compensation semantics. **Decision needed:** is this acceptable, or do we need a cross-shard 2PC mechanism? Probability of partial failure is very low (would require uncorrelated failures); 2PC would add latency to every multi-shard call.

**[OPEN-2] Reservation expiry granularity.** Under reservation semantics, a held reservation blocks capacity for other consumers. If a user backend hangs (slow query, network issue), its reservations are held until the user tx aborts. **Decision needed:** is there a maximum held duration we want to enforce (e.g., 60s), and what's the behavior on expiry (release-and-fail-pending-confirms, or release-and-retry-confirm-as-fresh)?

**[OPEN-3] Periodic method finalization concurrency.** Period close UPDATEs consumption rows. If a new consumption arrives during period close (race between "period ended" wall clock and "period close run"), is it in the closing period or the new period? **Decision needed:** explicit period-boundary semantics (e.g., consumed_at < period_end is in the period, regardless of when the close runs).

**[OPEN-4] Standard cost variance booking.** Where do variance entries land? A separate `cost_variances` table is sketched but not specified. **Decision needed:** schema, posting frequency, integration with external GL.

**[OPEN-5] Method config schema.** The trait accepts `JsonValue` for method config; no schema enforcement. **Decision needed:** typed config per method (more boilerplate but typesafe) vs. JSON with runtime validation (more flexible). Current sketch is JSON; revisit if config errors become a source of bugs.

**[OPEN-6] Shmem queue spillover.** When inline `config_override` exceeds 256 bytes, a spillover area is referenced but not specified. **Decision needed:** separate shmem allocator for variable-size data, or restrict configs to fit inline.

**[OPEN-7] Recon performance.** Joining `cost_consumptions` against external posting_lines on every recon run is potentially expensive. **Decision needed:** incremental recon (only check rows since last successful recon), or full scan with parallelism. Probably incremental with periodic full-scan for safety.

**[OPEN-8] Audit trail for layer event ordering.** Two backends create layer events for the same item at the same `born_at` microsecond. `born_seq` tiebreaks, but its assignment must be done by the committer (not the caller) to ensure global monotonicity. **Decision needed:** confirm born_seq is committer-assigned; verify ordering tests cover this.

**[OPEN-9] Methods that need cross-item state.** Some costing methods (e.g., assembly with BOM components) compute cost based on multiple items' states. The current trait is single-item. **Decision needed:** does the trait need a multi-item variant, or is BOM-based costing out of initial scope?

**[OPEN-10] Slot consumption_ids size.** `[AtomicI64; 32]` allows up to 32 consumption rows per request. A FIFO consume spanning 32+ layers exceeds this. **Decision needed:** raise the limit, allow spillover, or document the maximum spanning layers per request.

---

## 11. Migration path summary

Phases, executed sequentially with validation gates:

1. **Schema deployment.** New tables created in production alongside existing.
2. **Backfill.** Historical data migrated. Migration is one-time and offline.
3. **Extension installation.** Loaded via shared_preload_libraries. Initial settings: cache disabled, compensation semantics, conservative GUCs.
4. **Shadow mode.** Application calls new SQL surface in parallel with old; results compared. Discrepancies investigated.
5. **Cutover.** Application switches to new surface as sole writer. Old code paths archived.
6. **Tuning.** Enable cache, adjust shard count if needed, tune batch window for workload.
7. **Periodic method enablement.** After steady-state validation, enable periodic methods for applicable items.

Each phase has explicit rollback criteria.

---

## 12. Out of scope (explicit)

- Multi-primary write coordination.
- Cross-cluster logical replication.
- BOM/assembly cost roll-up.
- Cost rate variance tracking (separate from standard cost variance).
- Real-time GL posting (cost rows generate GL entries asynchronously).
- UI/UX of the costing administration.
- The application's posting_lines schema (out of extension control; only joined for recon).
