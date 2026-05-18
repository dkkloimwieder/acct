# PoC Validation Specification — v2.1 Async Lexicographical Ledger Queue

**Status:** PoC validation spec; gate for design-v2.1.md construction
**Target:** PostgreSQL 18+, pgrx 0.17+
**Companion document:** `design-v2.1.md` (reference architecture, deferred until this PoC passes)
**Audience:** PoC implementer, reviewer deciding whether to greenlight the full v2.1 build

---

## 0. Purpose and Scope

This document specifies what the v2.1 architecture must demonstrate before the full system described in `design-v2.1.md` is worth building. It is the validation gate.

**In scope:**
- The two-queue model (staging queue + committer queue) with the router as middleware.
- The Router: affinity grouping (union-find on pool_key overlap), fairness rule, single-BGWorker death and recovery.
- Two-domain lexicographical lock acquisition: SKU domain + WIP domain. Lock sets are the deduplicated union across all envelopes in a SuperBatch.
- The committer's pipeline: lex-lock acquisition, dedup-lookup, bounded snapshot hydration, in-memory dispatch, bulk UNNEST insertion.
- Bulk UNNEST writes at SuperBatch granularity, across multiple write target tables.
- Synchronous enqueue with `queue_full_timeout_ms` backpressure.
- User-tx coupling via Option (C): user_tx_xid stamped on envelope, committer checks pg_xact.
- Per-envelope failure isolation within a SuperBatch.
- Eventual resolution via `poc_v21_submission_status`.
- Three cost methods: FIFO, AVG, STD.
- Synthetic `wo_complete` event type exercising multi-pool atomicity (K components + 1 WIP + 1 output).
- Workload shapes including low-overlap, high-overlap, and hot-pool patterns.
- Theoretical analytical upper bounds for context.
- Measured throughput, latency, and failure-mode recovery on fixed PoC hardware.

**Out of scope:**
- WAC family methods (perpetual, periodic, retroactive). Close-hook DAG and variance routing. Deferred to design-v2.1.
- Posting_lines integration with acct's full schema. PoC writes to its own minimal tables (see §1.2).
- Multi-currency. Single currency assumed throughout.
- Lots, units, identity dimensions. Pool identity is `(sku_id, location_id)` for SKU pools, `(work_order_id, operation_id)` for WIP pools.
- Analytical dimensions (routing_op, project, etc.). No dimension reads in plan_apply.
- Webhook delivery. Terminal state observable via `poc_v21_submission_status` table only.
- User-tx coupling Options (A) PRE_COMMIT XactCallback and (B) push-and-forget. Only Option (C) implemented.
- BOM expansion logic. Caller submits already-expanded pool_keys; PoC's `wo_complete` event carries a payload with K random components and is treated as expanded.
- Multi-cost-book.
- Replication, HA, multi-tenancy.
- The alternatives flagged in design-v2.1 §14. PoC uses the defaults; alternatives are deferred to follow-up experiments.

**Validation outcome:** at completion, this document carries a verdict (pass / fail / conditional pass with caveats) plus a pinned `V21_BENCHMARK_RESULTS.md` documenting measured throughput surface, p99 latencies, router affinity-grouping statistics (component-size distribution, cross-SuperBatch wait time), and failure-mode recovery times on PoC hardware. That artifact is the input to deciding whether design-v2.1 construction starts.

---

## 1. PoC Architecture and Schema

### 1.1 What the PoC implements

```
┌────────────────────────────────────────────────────────────────────┐
│  Caller backends (psql clients)                                    │
│  - SELECT poc_v21_enqueue(correlation_id, event_type, payload,     │
│                            pool_keys, durable_queue := false)      │
│  - durable_queue=true requires persistent staging (see §1.10);     │
│    raises if requested without persistent_staging enabled          │
│  - Synchronous: blocks up to queue_full_timeout_ms                 │
│  - Returns void on success; raises ERRCODE on timeout              │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│  Staging queue (shmem)                                             │
│  - StagingEntry × N (default 16384)                                │
│  - 5-state machine: empty/pending/processing/routed/abandoned      │
│  - Per-queue LWLock for head/tail                                  │
│  - Backpressure condition variable                                 │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│  Router (single BGWorker)                                          │
│  - Reads window of pending staging entries                         │
│  - Affinity grouping: envelopes sharing any pool_key are routed    │
│    together into one SuperBatch (union-find on overlap)            │
│  - Independent groups become independent SuperBatches              │
│   (WIP pool_keys participate in affinity — see §1.5)               │
│  - Fairness backstop: head-of-queue forced after starvation_ticks  │
│  - Assembles SuperBatch in local memory                            │
│  - Pushes to committer queue, then CAS staging entries to routed   │
│  - Recovery sweep on BGWorker restart                              │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│  Committer queue (shmem)                                           │
│  - CommitterQueueEntry × M (default 2048)                          │
│  - 4-state machine: empty/ready/in_flight/completed                │
│  - CAS-based committer election                                    │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│  Committer worker pool                                             │
│  - Each: CAS-claims a SuperBatch                                   │
│  - Step 1: Lex-sort pool_keys (SKU + WIP separately)               │
│  - Step 2: StartTransactionCommand, FOR UPDATE on lock rows        │
│  - Step 2.5: Dedup-lookup against existing cost rows               │
│  - Step 3: Bounded snapshot hydration (per-pool LIMIT 1000)        │
│  - Step 4: In-memory dispatch through trait, accumulate row vecs   │
│  - Step 5: Bulk UNNEST INSERT across all write targets             │
│  - CommitTransactionCommand (single fsync); update status; free    │
│    slots                                                           │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│  PoC cost and accounting tables (append-only writes + UPSERTs)     │
│  Step 5 write targets (6 statements per SuperBatch in non-durable; │
│  7 when at least one durable_queue envelope is present):           │
│  - poc_v21_cost_layers      (FIFO + AVG layer state)               │
│  - poc_v21_cost_depletions  (FIFO depletions, layer-attributed)    │
│  - poc_v21_cost_consumptions (AVG + STD consumptions)              │
│  - poc_v21_posting_lines    (event log; exercises multi-target     │
│                              UNNEST)                               │
│  - poc_v21_posting_line_inventory (qty-side detail)                │
│  - poc_v21_avg_pool_state   (running average; UPSERTed by Step 5)  │
│  Support tables (read by Step 3, configured at setup):             │
│  - poc_v21_standard_costs   (STD-method unit costs)                │
│  - poc_v21_sku_method_assignments (per-SKU cost method)            │
│  Lock-domain tables (FOR UPDATE rows, lazy-created):               │
│  - poc_v21_pool_locks       (SKU lock domain)                      │
│  - poc_v21_wip_pool_locks   (WIP lock domain)                      │
│  Status / durability:                                              │
│  - poc_v21_submission_status (terminal state observation)          │
│  - poc_v21_persistent_staging (durable-queue envelopes; populated  │
│                                only when persistent_staging=on)    │
└────────────────────────────────────────────────────────────────────┘
```

### 1.2 PoC schema (minimal; not acct's schema)

```sql
-- Cost tables: minimal analogs of acct's cost machinery.

CREATE TABLE poc_v21_cost_layers (
    layer_id        BIGSERIAL PRIMARY KEY,
    sku_id          BIGINT NOT NULL,
    location_id     BIGINT NOT NULL,
    qty             BIGINT NOT NULL,             -- signed
    unit_cost       BIGINT NOT NULL,
    born_at         TIMESTAMPTZ NOT NULL,
    born_seq        BIGINT NOT NULL,             -- committer-assigned, monotone per pool
    source_kind     TEXT NOT NULL,               -- 'receipt' | 'wo_output' | 'adjustment'
    source_ref      BIGINT,
    correlation_id  UUID NOT NULL,
    user_tx_xid     xid8 NOT NULL,
    committer_tx_id BIGINT NOT NULL,
    superbatch_id   BIGINT NOT NULL
);
CREATE INDEX poc_v21_cost_layers_pool ON poc_v21_cost_layers (sku_id, location_id, born_at, born_seq);
CREATE INDEX poc_v21_cost_layers_correlation ON poc_v21_cost_layers (correlation_id);

CREATE TABLE poc_v21_cost_depletions (
    depletion_id    BIGSERIAL PRIMARY KEY,
    layer_id        BIGINT NOT NULL REFERENCES poc_v21_cost_layers(layer_id),
    qty             BIGINT NOT NULL CHECK (qty > 0),
    unit_cost       BIGINT NOT NULL,
    consumed_at     TIMESTAMPTZ NOT NULL,
    consumed_seq    BIGINT NOT NULL,             -- monotone per layer
    issue_id        BIGINT NOT NULL,
    method_used     TEXT NOT NULL,               -- 'fifo'
    correlation_id  UUID NOT NULL,
    user_tx_xid     xid8 NOT NULL,
    committer_tx_id BIGINT NOT NULL,
    superbatch_id   BIGINT NOT NULL,
    UNIQUE (issue_id, method_used, layer_id)
);
CREATE INDEX poc_v21_cost_depletions_layer ON poc_v21_cost_depletions (layer_id);
CREATE INDEX poc_v21_cost_depletions_issue ON poc_v21_cost_depletions (issue_id);

CREATE TABLE poc_v21_cost_consumptions (
    consumption_id  BIGSERIAL PRIMARY KEY,
    sku_id          BIGINT NOT NULL,
    location_id     BIGINT NOT NULL,
    qty             BIGINT NOT NULL CHECK (qty > 0),
    applied_unit_cost BIGINT NOT NULL,
    consumed_at     TIMESTAMPTZ NOT NULL,
    consumed_seq    BIGINT NOT NULL,
    issue_id        BIGINT NOT NULL,
    method_used     TEXT NOT NULL,               -- 'avg' | 'std'
    correlation_id  UUID NOT NULL,
    user_tx_xid     xid8 NOT NULL,
    committer_tx_id BIGINT NOT NULL,
    superbatch_id   BIGINT NOT NULL,
    UNIQUE (issue_id, method_used)
);
CREATE INDEX poc_v21_cost_consumptions_pool ON poc_v21_cost_consumptions (sku_id, location_id, consumed_at);

-- Posting tables: minimal analogs of acct's posting machinery, included to
-- exercise multi-target bulk UNNEST writes from one committer top-level tx.

CREATE TABLE poc_v21_posting_lines (
    posting_line_id BIGSERIAL PRIMARY KEY,
    business_date   DATE NOT NULL,
    doc_chrono      BIGINT NOT NULL,
    document_id     BIGINT NOT NULL,
    sub_priority    INT NOT NULL DEFAULT 0,
    event_type      TEXT NOT NULL,                -- 'wo_complete' | 'inv_adjust' | etc.
    amount          BIGINT NOT NULL,              -- signed
    debit_account   BIGINT,                       -- nullable for some event types
    credit_account  BIGINT,
    correlation_id  UUID NOT NULL,
    user_tx_xid     xid8 NOT NULL,
    committer_tx_id BIGINT NOT NULL,
    superbatch_id   BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX poc_v21_posting_lines_chrono ON poc_v21_posting_lines (business_date, doc_chrono);
CREATE INDEX poc_v21_posting_lines_correlation ON poc_v21_posting_lines (correlation_id);

CREATE TABLE poc_v21_posting_line_inventory (
    posting_line_id BIGINT NOT NULL REFERENCES poc_v21_posting_lines(posting_line_id),
    sku_id          BIGINT NOT NULL,
    location_id     BIGINT NOT NULL,
    qty             BIGINT NOT NULL,              -- signed
    layer_id        BIGINT,                       -- nullable for STD consumption
    PRIMARY KEY (posting_line_id, sku_id, location_id)
);

-- Status observation table

CREATE TABLE poc_v21_submission_status (
    correlation_id  UUID PRIMARY KEY,
    state           TEXT NOT NULL CHECK (state IN ('queued', 'processing', 'committed', 'failed', 'replayed')),
    enqueued_at     TIMESTAMPTZ NOT NULL,
    processed_at    TIMESTAMPTZ,
    committed_at    TIMESTAMPTZ,
    error_code      TEXT,
    error_detail    JSONB,
    committer_tx_id BIGINT,
    superbatch_id   BIGINT
);
CREATE INDEX poc_v21_submission_status_state ON poc_v21_submission_status (state) WHERE state IN ('queued', 'processing');
CREATE INDEX poc_v21_submission_status_enqueued ON poc_v21_submission_status (enqueued_at);

-- Lock domains: separate tables per design-v2.1 §2

CREATE TABLE poc_v21_pool_locks (
    sku_id       BIGINT NOT NULL,
    location_id  BIGINT NOT NULL,
    lock_version BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (sku_id, location_id)
);

CREATE TABLE poc_v21_wip_pool_locks (
    work_order_id BIGINT NOT NULL,
    operation_id  BIGINT NOT NULL,
    lock_version  BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (work_order_id, operation_id)
);

-- Method assignments per SKU (cached per-committer)

CREATE TABLE poc_v21_sku_method_assignments (
    sku_id    BIGINT NOT NULL PRIMARY KEY,
    method_id TEXT NOT NULL CHECK (method_id IN ('fifo', 'avg', 'std'))
);

CREATE TABLE poc_v21_standard_costs (
    sku_id         BIGINT NOT NULL,
    location_id    BIGINT NOT NULL,
    unit_cost      BIGINT NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sku_id, location_id, effective_from)
);

-- AVG pool state: incrementally maintained running average. Read by Step 3
-- snapshot hydration; UPSERTed by Step 5 bulk insert. This is how production
-- WAC perpetual actually works; reconstructing the average from
-- posting_lines history on every event would be O(history_size) and
-- defeats the purpose of a running aggregate.
CREATE TABLE poc_v21_avg_pool_state (
    sku_id               BIGINT NOT NULL,
    location_id          BIGINT NOT NULL,
    avg_unit_cost        BIGINT NOT NULL,           -- running average
    avg_total_qty        BIGINT NOT NULL,           -- signed; tracks pool depth for divisor and bound checks
    last_updated_at      TIMESTAMPTZ NOT NULL,
    last_committer_tx_id BIGINT NOT NULL,
    PRIMARY KEY (sku_id, location_id)
);
```

**Caller contract:** the PoC's enqueue function requires the caller to supply the complete R/W set. Callers (the test harness) compute the complete `pool_keys` array before calling `poc_v21_enqueue`. There is no PoC-side BOM expansion; the test harness generates pool_keys directly for each synthetic event.

### 1.3 Cost method implementations

Three trait implementations, identical in shape to v2's PoC but bound to v2.1's pipeline:

```rust
pub trait PocV21CostMethod: Send + Sync + 'static {
    fn method_id(&self) -> &'static str;

    /// Pure, deterministic. No SPI, no shmem mutation.
    fn plan_apply(
        &self,
        events: &[PocV21Event],
        snapshot: &mut PocV21Snapshot,
    ) -> PocV21ApplyResult;
}

pub struct PocV21Event {
    pub event_seq: u64,
    pub correlation_id: Uuid,
    pub issue_id: i64,
    pub event_type: PocV21EventType,
    pub sku_id: i64,
    pub location_id: i64,
    pub qty: i64,
    pub at: Timestamp,
    pub business_date: Date,
    pub doc_chrono: i64,
    pub document_id: i64,
    pub sub_priority: i32,
    pub wo_completion_payload: Option<WoCompletionPayload>,
}

pub enum PocV21EventType {
    InvAdjust,
    InvIssue,
    PoReceipt,
    SoShipment,
    WoComplete,
}

pub struct WoCompletionPayload {
    pub work_order_id: i64,
    pub operation_id: i64,
    pub components: Vec<(i64, i64, i64)>,  // (sku_id, location_id, qty)
    pub wip_account: (i64, i64),           // (wo_id, op_id)
    pub output: (i64, i64, i64),           // (sku_id, location_id, qty)
}

pub struct PocV21Snapshot {
    pub sku_pools: HashMap<(i64, i64), SkuPoolState>,    // (sku, location) -> state
    pub wip_pools: HashMap<(i64, i64), WipPoolState>,    // (wo, op) -> state
    pub method_assignments: HashMap<i64, &'static str>,  // sku_id -> method
    pub born_seq_seeds: HashMap<(i64, i64), i64>,
    pub consumed_seq_seeds: HashMap<i64, i64>,           // layer_id -> max consumed_seq
}

pub struct SkuPoolState {
    pub layers: Vec<LayerView>,            // FIFO order; effective_qty > 0. Populated for FIFO-method pools.
    pub avg_unit_cost: Option<i64>,        // For AVG-method SKUs; loaded from poc_v21_avg_pool_state.
    pub avg_total_qty: Option<i64>,        // For AVG-method SKUs; signed pool depth; loaded from poc_v21_avg_pool_state.
                                           // After in-memory mutation by plan_apply, the new avg_unit_cost and
                                           // avg_total_qty are UPSERTed into the table in Step 5.
    pub standard_cost: Option<i64>,        // For STD-method SKUs; loaded from poc_v21_standard_costs.
}

pub struct WipPoolState {
    pub accumulated_value: i64,
}

pub struct PocV21ApplyResult {
    pub per_event: Vec<PocV21EventResult>,
    pub envelope_error: Option<PocV21EnvelopeError>,    // Set when the envelope as a whole fails
                                                        // (e.g., before any events could be processed,
                                                        // or when a method aborts mid-envelope).
    pub layer_inserts: Vec<PocV21LayerRow>,
    pub depletion_inserts: Vec<PocV21DepletionRow>,
    pub consumption_inserts: Vec<PocV21ConsumptionRow>,
    pub posting_line_inserts: Vec<PocV21PostingLineRow>,
    pub posting_line_inventory_inserts: Vec<PocV21PostingLineInventoryRow>,
}

pub struct PocV21EnvelopeError {
    pub error_code: &'static str,    // e.g., "InsufficientInventory", "MethodMismatch"
    pub error_detail: serde_json::Value,
}
```

**FIFO** method: walks the SKU pool's layers in `(born_at, born_seq)` order. Emits depletion rows. For `WoComplete`, processes each component as a separate FIFO consumption, accumulates total component cost, emits an output layer at the computed unit cost.

**AVG** method (running weighted-average): reads `avg_unit_cost` and `avg_total_qty` from the SKU pool state (loaded by Step 3 from `poc_v21_avg_pool_state`).

- **On consumption** (negative qty event, e.g., `InvIssue` or `SoShipment`): emits a consumption row at the current `avg_unit_cost`. Decrements `avg_total_qty` in the in-memory snapshot by the consumed qty. Does NOT change `avg_unit_cost` (a consumption is at the existing average; only receipts can shift it).
- **On receipt** (positive qty event, e.g., `PoReceipt` or `InvAdjust` with positive qty, or a `WoComplete` output): the running average is updated:

  ```
  new_total_value = (avg_unit_cost × avg_total_qty) + (receipt_qty × receipt_unit_cost)
  new_total_qty = avg_total_qty + receipt_qty
  IF new_total_qty == 0:
      new_avg_unit_cost = avg_unit_cost   (preserve prior average; pool is empty,
                                            average is undefined but kept stamped
                                            on the pool so the next receipt has
                                            a sane starting point)
  ELSE:
      new_avg_unit_cost = new_total_value / new_total_qty   (with appropriate rounding)
  ```

  Edge case: `new_total_qty == 0` arises when a receipt exactly offsets a prior negative pool balance (the pool was at -receipt_qty before this receipt). Mathematically the average is undefined when there's no qty to average over; the implementation MUST guard against the divide-by-zero panic and preserve the prior average. When the NEXT receipt arrives against a zero-depth pool, `new_total_qty = 0 + next_receipt_qty`, and `new_avg_unit_cost = (avg_unit_cost × 0 + next_receipt_qty × next_unit_cost) / next_receipt_qty = next_unit_cost` — so a single receipt resets the pool's average to that receipt's unit cost, which is the correct behavior.

  Caller-bug edge case: `receipt_qty == 0` AND `avg_total_qty == 0`. This is a degenerate receipt (zero qty into an empty pool). The implementation should reject this at the AVG plan_apply level with `error_code='ZeroQtyReceipt'`; legitimate workloads should never emit such events.

  Emits a layer row (for traceability, even though AVG doesn't consume layers individually). Mutates `avg_unit_cost` and `avg_total_qty` in the snapshot.
- **For `WoComplete`**: components are consumed (per the consumption rule above, each at its OWN SKU's avg_unit_cost — which may differ per component). Total component cost = `Σ (consumed_qty × component_avg_unit_cost)`. The output is a receipt at `output_unit_cost = total_component_cost / output_qty`, which updates the output SKU's running average via the receipt rule. The implementation MUST guard `output_qty == 0` (a WO that produces nothing): reject with `error_code='ZeroQtyOutput'`. A legitimate WO never produces zero output.
- **Step 5** UPSERTs the mutated `avg_unit_cost` and `avg_total_qty` for every SKU pool that AVG touched. The lex-lock on `pool_locks` ensures no concurrent committer modifies the same pool, so the running average is race-free. **CRITICAL:** the bulk array passed to the UPSERT MUST contain exactly one entry per (sku_id, location_id) pool — the FINAL state after ALL events in the SuperBatch have applied to that pool's in-memory snapshot. If the committer naively appends snapshot state per-event, the same pool can appear multiple times in the array, and PG will abort the entire SuperBatch with `ERROR: ON CONFLICT DO UPDATE command cannot affect row a second time`. The committer's array-builder MUST deduplicate by (sku_id, location_id) before invoking Step 5, taking the final snapshot state for each pool.

**STD** method (standard cost): reads `standard_cost` from `poc_v21_standard_costs` (loaded by Step 3 with the latest effective_from). Emits a consumption row per consumption event at standard cost. Does NOT track pool depth (no `avg_total_qty` equivalent); standard cost permits negative pool qty by accounting policy. For `WoComplete`, computes total component cost = `Σ (qty × component_standard_cost)`, emits output layer at `total / output_qty`. Same `output_qty == 0` guard applies as for AVG. The output unit cost is what the WO actually consumed (component standard costs); variance to the output's own standard cost is acct's concern, not the extension's.

For methods that consume from the same SKU multiple times in one batch (e.g., two events both consuming SKU X), `plan_apply` mutates `snapshot.sku_pools[X]` between events so the second sees the first's effect.

### 1.4 PoC trait dispatch

```rust
fn dispatch_event(event: &PocV21Event, snapshot: &PocV21Snapshot) -> &'static dyn PocV21CostMethod {
    // For WoComplete: the output SKU's method drives dispatch (mirrors R2 credit-first)
    let driving_sku = match event.event_type {
        PocV21EventType::WoComplete => event.wo_completion_payload.as_ref().unwrap().output.0,
        _ => event.sku_id,
    };
    match snapshot.method_assignments[&driving_sku] {
        "fifo" => &FIFO_METHOD,
        "avg"  => &AVG_METHOD,
        "std"  => &STD_METHOD,
        _ => panic!("unknown method"),
    }
}
```

The PoC's WoComplete handler iterates through components; each component's consumption uses the component's own method (looked up per-component). Only the output's method is the "driving" method for posting-line construction.

### 1.5 SuperBatch composition and lock domain handling

A SuperBatch contains envelopes the router has determined must be processed together because they share state (pool_keys). The router actively **groups envelopes by pool-key overlap** so that envelopes touching the same pool are routed to the same committer.

**The affinity rule:** envelopes A and B are in the same SuperBatch if they share at least one pool_key, OR if there is a chain of envelopes C₁, C₂, ..., Cₙ where A shares a pool_key with C₁, C₁ with C₂, ..., Cₙ with B. In graph terms: the SuperBatch is one connected component of the overlap graph on the router's window.

Implementation: union-find. For each envelope in the window, for each pool_key it carries, union it with any other envelope that has previously touched the same pool_key. Each connected component becomes one SuperBatch.

**Why active grouping rather than FIFO:** under concurrent submission from many backends, envelopes that share pool_keys do NOT arrive consecutively in the staging queue. Example: queue arrival is [Env_A on SKU 5, Env_B on SKU 6, Env_C on SKU 5]. FIFO packing with batch_size_max=2 would put {Env_A, Env_B} in SB-1 and {Env_C} in SB-2. The two SKU-5 envelopes land in different SuperBatches. Different committers acquire FOR UPDATE on SKU 5 — one wins, the other waits. The waiting committer holds its top-level transaction open and other locks while waiting. This is the inter-committer FOR UPDATE pathology v2.1 exists to avoid.

Active grouping puts Env_A and Env_C into the same SuperBatch (they share SKU 5). One committer processes both sequentially against a shared in-memory snapshot. No cross-committer wait. The independent Env_B goes into a separate SuperBatch and runs in parallel.

**Splitting oversized components:** if a connected component contains more than `batch_size_max` envelopes, the component is split into chunks of size `batch_size_max`. The chunks become separate SuperBatches. Cross-SuperBatch contention via PG's row locks on `pool_locks` serializes them correctly — this is the fallback for genuinely-oversized contention groups. For workloads without sustained hot pools, splitting is uncommon under default `batch_size_max=50`; for hot-pool workloads (shape S4 fan_in, S5 hot_pool), splitting is the rule, and the §2.1 B5 ceiling applies on the contended pool.

**SKU domain:** the committer's Step 2 acquires FOR UPDATE on the deduplicated union of all envelopes' `sku_pool_keys`. Overlapping envelopes within the SuperBatch produce one lock per pool, not multiple.

**WIP domain:** WIP pools are identified by `(work_order_id, operation_id)`. WIP pool_keys participate in the affinity grouping the same way SKU pool_keys do — two envelopes touching the same WIP pool are routed to the same SuperBatch.

The committer acquires FOR UPDATE on the deduplicated union of both domains' pool_keys in Step 2: SKU domain first (lex-sorted), then WIP domain (lex-sorted). This ordering convention prevents deadlocks between SuperBatches that span a split component.

**Within-SuperBatch ordering:** envelopes within a SuperBatch are processed in arrival order (request_seq). Events within the SuperBatch are then chronologically sorted (per §1.8 Step 4) for plan_apply. This preserves causal ordering across same-caller envelopes that contributed events touching shared pools.

### 1.6 Shmem layout

```rust
const POC_V21_STAGING_QUEUE_SIZE: u32 = 16384;
const POC_V21_COMMITTER_QUEUE_SIZE: u32 = 2048;
const POC_V21_SPILLOVER_ARENA_MB: u32 = 128;

#[repr(C, align(64))]
pub struct StagingQueue {
    pub head: AtomicU32,
    pub tail: AtomicU32,
    pub lock_tranche_id: u32,
    pub _pad: [u8; 4],
    pub next_request_seq: AtomicU64,
    pub backpressure_cv_tranche_id: u32,
    pub _pad2: [u8; 4],
    // entries[POC_V21_STAGING_QUEUE_SIZE] follow
}

#[repr(C)]
pub struct StagingEntry {
    pub valid: AtomicU8,              // 0=empty, 1=pending, 2=processing, 3=routed, 4=abandoned
    pub _pad: [u8; 7],
    pub request_seq: u64,
    pub correlation_id: [u8; 16],
    pub user_tx_xid: u64,
    pub event_type_id: u16,
    pub payload_offset: u32,
    pub payload_length: u32,
    pub sku_pool_count: u16,           // SKU pool count; committer dedups union across batch
    pub wip_pool_count: u16,           // WIP pool count; committer dedups union across batch
    pub sku_pool_keys_offset: u32,
    pub wip_pool_keys_offset: u32,
    pub enqueued_at_micros: u64,
    pub backend_pid: i32,
    pub superbatch_id: AtomicU64,      // Set with Release semantics by router before CAS valid: 2→3.
                                       // Read with Acquire semantics by recovery sweep AFTER confirming valid==3.
                                       // Classic data-before-flag pattern; ordering is load-bearing for sweep correctness.
    pub eject_count: AtomicU32,        // Incremented each time committer ejects (caller-tx in_progress).
                                       // Widened to u32 so max_eject_count can sit comfortably above the
                                       // wall-clock bound (~1500-6000 ejects at 30s/5-20ms-per-cycle).
    pub _pad2: [u8; 4],
}

#[repr(C, align(64))]
pub struct CommitterQueue {
    pub head: AtomicU32,
    pub tail: AtomicU32,
    pub lock_tranche_id: u32,
    pub _pad: [u8; 4],
    pub next_superbatch_id: AtomicU64,
    // entries[POC_V21_COMMITTER_QUEUE_SIZE] follow
}

#[repr(C)]
pub struct CommitterQueueEntry {
    pub valid: AtomicU8,                  // 0=empty, 1=ready, 2=in_flight, 3=completed
    pub _pad: [u8; 7],
    pub superbatch_id: u64,
    pub envelope_count: u16,
    pub staging_entry_offsets: u32,       // Spillover-arena array of staging indices
    pub sku_pool_keys_offset: u32,        // Deduplicated, sorted across all envelopes
    pub sku_pool_keys_count: u16,
    pub wip_pool_keys_offset: u32,
    pub wip_pool_keys_count: u16,
    pub committer_bgw_slot: AtomicU32,    // Index into BackgroundWorkerData slot array
                                          // (the committer's own registered-worker slot).
                                          // Combined with committer_bgw_generation, uniquely
                                          // identifies a running committer process — safe
                                          // against OS PID recycling.
                                          // Sentinel 0xFFFFFFFF = "no committer claimed yet."
    pub committer_bgw_generation: AtomicU32,  // Generation counter from BackgroundWorkerData
                                          // at claim time. PG bumps the generation each time
                                          // a worker slot is reused; a stale (slot, generation)
                                          // pair means the original committer is gone.
    pub committer_acquired_at_ns: AtomicU64,
    pub committer_tx_id: AtomicU64,
    pub enqueued_at_micros: u64,
}
```

**Note on committer identity.** Raw OS PIDs are unreliable for liveness checks in containerized or high-process-churn environments — a recycled PID can falsely report a dead committer as alive via `kill(pid, 0)`. Instead, the CommitterQueueEntry stores the committer's BGWorker slot index (its position in PG's BackgroundWorkerData shared-memory array) and the generation counter from that slot at claim time. The pair is unique across a postmaster's lifetime; a stale pair (one whose generation has advanced, or whose slot is now empty) unambiguously means the original committer is gone.

Liveness check sequence:
1. Read `(slot, generation)` from CommitterQueueEntry.
2. Look up `BackgroundWorkerData.slot[slot]`.
3. If slot is in use AND current generation matches stored generation → committer is alive.
4. Otherwise (slot empty, or generation advanced) → committer is dead.

This replaces `kill(committer_pid, 0)` everywhere it appears in recovery and audit paths. The internal PG helper `GetBackgroundWorkerPid(handle, &pid)` can also be used when a BGWorker registration handle is available.

### 1.7 GUCs

| GUC | Default | Range | Reload |
|-----|---------|-------|--------|
| `poc_v21.staging_queue_size` | 16384 | 1024-262144 | Postmaster |
| `poc_v21.committer_queue_size` | 2048 | 256-16384 | Postmaster |
| `poc_v21.spillover_arena_mb` | 128 | 16-1024 | Postmaster |
| `poc_v21.queue_full_timeout_ms` | 5000 | 100-60000 | Sighup |
| `poc_v21.committer_lease_ms` | 100 | 10-10000 | Sighup |
| `poc_v21.router_window_size` | 1000 | 100-10000 | Sighup |
| `poc_v21.batch_size_max` | 50 | 1-1000 envelopes | Sighup |
| `poc_v21.batch_window_us` | 500 | 50-50000 | Sighup |
| `poc_v21.router_starvation_threshold_ticks` | 10 | 1-100 | Sighup |
| `poc_v21.snapshot_layer_limit_per_pool` | 1000 | 100-100000 | Sighup |
| `poc_v21.max_eject_count` | 10000 | 100-1000000 | Sighup |
| `poc_v21.caller_tx_timeout_ms` | 30000 | 1000-3600000 | Sighup |
| `poc_v21.committer_count` | 4 | 1-64 | Sighup |
| `poc_v21.status_insert_mode` | caller_intx | caller_intx \| committer_lazy (committer_lazy requires persistent_staging=on; see §3.4) | Sighup |
| `poc_v21.persistent_staging` | off | off \| on (gates `durable_queue=true` and unlocks `committer_lazy` mode) | Postmaster |
| `poc_v21.persistent_staging_gc_retention_hours` | 24 | 1-720 | Sighup |

**Startup validation:**
- `status_insert_mode = committer_lazy` requires `persistent_staging = on`. The extension fails to load if `committer_lazy` is set with `persistent_staging = off`. Hint: "committer_lazy requires persistent_staging=on to be safe under postmaster restart."
- `durable_queue=true` requires `persistent_staging = on`. The enqueue function raises ERRCODE_FEATURE_NOT_SUPPORTED if called with `durable_queue=true` while `persistent_staging=off`.
- `max_eject_count` is the safety bound on internal cycling for a single envelope; it caps work spent on hopeless cases. The wall-clock `caller_tx_timeout_ms` is the user-facing contract for how long a caller's user-tx can remain in_progress. With default `caller_tx_timeout_ms = 30s` and observed eject cycle times of ~5-20ms per cycle, the wall-clock timeout fires after ~1500-6000 ejects under sustained ejection. Default `max_eject_count = 10000` sits comfortably above that, so the wall-clock bound is the primary one in practice. The eject counter is a defensive backstop against pathological cycling (e.g., a tight cycle that somehow misses wall-clock checks). The `eject_count` field is widened to AtomicU32 in the StagingEntry struct so that values above the AtomicU16 ceiling of 65535 are representable.
- **Calibration check at startup.** The extension computes `theoretical_max_ejects = caller_tx_timeout_ms × 1000 / batch_window_us` (the upper bound on how many ejects a single envelope could accumulate if every router tick cycled it). If `theoretical_max_ejects > max_eject_count / 2`, log a WARNING: "max_eject_count may not be the primary bound under tight cycling; verify the wall-clock timeout is the intended bound, or raise max_eject_count to >= 2 × theoretical_max_ejects". This catches the case where an operator tunes batch_window_us down or caller_tx_timeout_ms up without proportionally raising max_eject_count.
- `committer_lease_ms` should be set per §5.1 pre-bake-off calibration (`max(100, 10 × fsync_p99_ms)`); the GUC default of 100ms assumes NVMe-class storage.
- The GUC catch-all validator (raising `ERRCODE_INVALID_PARAMETER_VALUE` for unrecognized enum values) handles invalid values for `status_insert_mode`, including the historical `caller_subtx` string — no named rejection branch is added; this is a greenfield project and the dead-code maintenance footprint of a deprecated-mode string in the validator is unwarranted.

**Operational guidance for `caller_tx_timeout_ms`:** set `caller_tx_timeout_ms <= deployment's statement_timeout`. The committer's eject-bound triggers at `caller_tx_timeout_ms`; PG's `statement_timeout` triggers at a (possibly longer) value. Configuring the former ≤ the latter ensures committer-initiated 'failed' status rows appear before PG's tx termination, giving operators bounded failure-visibility latency. The PoC default `caller_tx_timeout_ms = 30s` reflects the realistic upper bound for user-tx duration in costing-ledger workloads (operator-driven UI clicks complete in <1s; batch jobs and MRP runs complete within seconds). Deployments with longer-running callers can raise the GUC, but should also raise `max_eject_count` proportionally if cycling pressure becomes a concern.

### 1.8 Request lifecycle (end-to-end)

**Caller backend (enqueue path):**

```
 1. Construct envelope: correlation_id, event_type, payload, sku_pool_keys,
    wip_pool_keys, durable_queue (default false).
 2. Validate durable_queue request:
      If durable_queue=true AND poc_v21.persistent_staging = off:
        Raise ERRCODE_FEATURE_NOT_SUPPORTED with hint.
 3. Force user-tx XID allocation: user_tx_xid = pg_sys::GetCurrentTransactionId().
      (Always returns a valid XID; allocates one if not yet assigned.
       Does not raise. The wrapper in pgrx is `pg_sys::FullTransactionId`.)
 4. (Mode-dependent — see §3.4) Insert submission_status row:
      If status_insert_mode = 'caller_intx' (PoC default): INSERT inside caller's
        user-tx. Status rolls back with caller; committer's lazy fallback creates
        a row on aborted-user-tx detection. Correct under postmaster restart
        because all committed callers have durable status rows.
      If status_insert_mode = 'committer_lazy' (requires persistent_staging=on):
        SKIP this step. Committer creates the row when it determines terminal
        state. Recovery sweep can re-derive 'queued' state from
        poc_v21_persistent_staging if needed.
 5. If durable_queue = true: INSERT row into poc_v21_persistent_staging
    (correlation_id, user_tx_xid, event_type, payload, sku_pool_keys,
     wip_pool_keys, business_date, state='staged') within caller's user-tx.
    (WAL-logged; rides with the caller's normal commit, no extra fsync.)
 6. Allocate spillover-arena blocks for payload, sku_pool_keys, wip_pool_keys.
 7. Acquire staging queue LWLock, find a free StagingEntry.
      If queue full → CV-wait up to queue_full_timeout_ms.
      If timeout → release lock, raise ERRCODE_INSUFFICIENT_RESOURCES.
 8. Write StagingEntry fields. CAS valid: 0 → 1 (pending).
    NOTE: under durable_queue=true, the StagingEntry stores a reference to
    the persistent_staging row (correlation_id is the join key).
 9. Advance tail. Release LWLock.
10. SetLatch on router (router waits on its own latch when queue is empty).
11. Return void to caller (within caller's user-tx; commit/rollback is caller's
    decision).
```

**Router BGWorker (packing loop):**

```
loop:
 1. If staging queue head == tail → WaitLatch up to batch_window_us.
 2. Read up to router_window_size pending entries from head. Call this set W.
 3. Track per-entry starvation_tick_count in router-local state for the
    head-of-queue backstop (an entry that has been competing for a SuperBatch
    slot but losing to races with other routers — N/A for single-router PoC
    but retained for multi-router future and for consistent behavior under
    backpressure).
 4. Affinity grouping pass:
      a. Initialize union-find UF over W (one node per candidate envelope).
      b. Build a HashMap<PoolKey, Vec<EnvelopeIdx>>:
           For each candidate c in W:
             For each pool_key k in c.sku_pool_keys ∪ c.wip_pool_keys:
               pool_to_envs[k].push(c.idx)
      c. For each pool_key k with |pool_to_envs[k]| >= 2:
           Let head = pool_to_envs[k][0].
           For each other_idx in pool_to_envs[k][1..]:
             UF.union(head, other_idx)
      d. Collect connected components from UF.
         Result: groups: HashMap<Root, Vec<EnvelopeIdx>>.
      e. Sort groups by min(request_seq) within each group. Oldest-first
         dispatch — fairness rule. Within each group, sort members by
         request_seq for in-arrival-order processing.
 5. For each group G (in oldest-first order):
      For each chunk of G of size batch_size_max (so an oversized group splits
      into multiple SuperBatches; the splits will see cross-SuperBatch FOR UPDATE
      contention via PG's row locks as a backstop):
         a. Begin SuperBatch assembly for chunk.
         b. For each candidate c in chunk (in request_seq order):
              Attempt CAS staging.valid: 1 → 2 (processing).
              If CAS success:
                Add c to SuperBatch.
              If CAS failed:
                Candidate was claimed by another router (N/A for single-router
                PoC; defensive for future multi-router) or already progressed.
                Skip — the union-find included this envelope under the
                assumption it was pending; if it has progressed, the affinity
                relationship still holds with the rest of the group (the
                already-progressed envelope is now in some other SuperBatch,
                which will FOR UPDATE-serialize with this one as the fallback).
         c. If SuperBatch is empty after the chunk, continue.
         d. Compute the deduplicated union of all envelopes' sku_pool_keys
            within the SuperBatch, sorted in lex order. Same for wip_pool_keys.
         e. Allocate next superbatch_id from CommitterQueue.next_superbatch_id.
         f. Acquire CommitterQueue LWLock. Find free CommitterQueueEntry.
         g. Write CommitterQueueEntry fields (staging_offsets array in arena,
            dedup'd sorted sku_pool_keys and wip_pool_keys union arrays).
         h. CAS CommitterQueueEntry.valid: 0 → 1 (ready).
         i. Advance committer queue tail. Release LWLock.
         j. For each staging entry in SuperBatch (in request_seq order):
              Step 1: staging.superbatch_id.store(sb_id, Release)
                      (Release ordering ensures the superbatch_id write is
                       visible to any thread that subsequently observes
                       valid=3 via Acquire.)
              Step 2: staging.valid.compare_exchange(2, 3, Release, Relaxed)
                      (Release on success; the prior superbatch_id store is
                       now visible to any reader that sees valid=3 via Acquire.)
              CRITICAL: superbatch_id MUST be stored before the valid CAS.
              Reversing this ordering creates a window where a reader observes
              valid=3 but reads stale superbatch_id=0, leading to incorrect
              recovery decisions. The recovery sweep (§3.3) relies on this
              data-before-flag invariant.
         k. SetLatch on idle committer (broadcast).
 6. (Per design-v2.1 §6.4, if router dies between (h) and (j), the recovery
    sweep on next router boot detects this and leaves the entries for the
    committer to clean up.)
```

**Cost of affinity grouping:** Per §2.1 B3 (full analysis): for W=1000 (router_window_size default) and K=15 (average pool_keys per envelope), per-tick work is ~2-3ms (hashmap construction + union-find + window read + per-SuperBatch sort/dedup). Router ceiling: ~300-500 ticks/sec at full window saturation, ~300K-500K envelopes/sec aggregate. See §2.1 B3 for the breakdown. The router is not the binding constraint for any reasonable workload.

**Committer worker (claim and execute):**

```
On wake or new committer queue entry:
 1. Walk committer queue from head looking for valid=1 (ready) entries.
 2. Try CAS valid: 1 → 2 (in_flight) and store committer identity:
      atomically write (committer_bgw_slot = MyBgwSlot, committer_bgw_generation
      = MyBgwGeneration) into CommitterQueueEntry. MyBgwSlot is the calling
      BGWorker's slot index in PG's BackgroundWorkerData; MyBgwGeneration is
      the slot's current generation counter at claim time.
      If CAS fails (another committer won) → continue scanning.
      If CAS success → store committer_acquired_at_ns = now_ns.
 3. Read SuperBatch's staging entry offsets, sku_pool_keys, wip_pool_keys.
 4. For each staging entry's user_tx_xid:
      Check pg_xact_status(user_tx_xid).
        'committed' → keep envelope in batch.
        'aborted' → mark envelope's correlation_id as 'failed' with
                    error_code='caller_tx_aborted'; exclude from batch.
                    (Lazy status INSERT: under caller_intx mode the original
                     'queued' row was rolled back with the caller's user-tx,
                     so no row exists. Committer creates it now via:
                       INSERT INTO poc_v21_submission_status
                         (correlation_id, state, error_code, enqueued_at,
                          processed_at)
                       VALUES (...)
                       ON CONFLICT (correlation_id) DO NOTHING.
                     The ON CONFLICT DO NOTHING is load-bearing for safety
                     under re-routing: if an envelope is ejected and routed
                     to another committer that also detects the aborted
                     user-tx, both committers may attempt the lazy INSERT.
                     Idempotency via the PK conflict ensures no duplicates,
                     no errors.)
        'in_progress' → eject the envelope:
                    - Increment staging.eject_count.
                    - If now - enqueued_at_micros > caller_tx_timeout_ms × 1000
                      (primary wall-clock bound):
                        Mark envelope as 'failed' with
                        error_code='caller_tx_timeout'; exclude from batch.
                        (Lazy status INSERT with ON CONFLICT DO NOTHING
                         as above if needed.)
                    - Else if eject_count > max_eject_count (defensive safety
                      bound — should not fire before wall-clock under normal
                      operation):
                        Mark envelope as 'failed' with
                        error_code='caller_tx_eject_exhausted'; exclude from
                        batch.
                    - Else:
                        CAS staging.valid: 3 (routed) → 1 (pending).
                        CAS staging.superbatch_id: current → 0.
                        Exclude from current batch.
                        (The router will re-pick on a future tick.)
      CRITICAL: the committer NEVER sleeps waiting on a caller's user-tx.
      Ejection is immediate; the router's natural rescheduling handles
      eventual resolution. This is the single biggest correctness rule
      in v2.1's caller-tx coupling.
 5. If all envelopes in batch were caller-tx-aborted: skip Steps 5-9 below;
    go straight to status updates (Step 10) and slot cleanup.
 6. STEP 1 (sort): Build sorted, deduplicated sku_pool_keys + sorted, deduplicated
    wip_pool_keys arrays from the UNION of all envelopes' pool_keys in the
    SuperBatch. Overlapping envelopes contribute the same pool_key once; the
    arrays carry each unique pool key exactly once across the entire SuperBatch.
 7. STEP 2 (transaction begin + locks):
      StartTransactionCommand().  // Opens a top-level transaction. WAL fsync
                                  // happens at the matching CommitTransaction
                                  // Command() in Step 13.
      SetTransactionIsolationLevel(READ_COMMITTED).
      (Top-level transaction runs at READ COMMITTED isolation. This is
       load-bearing for causal-chain correctness: when this committer's
       FOR UPDATE blocks behind another committer's FOR UPDATE for the same
       pool, PG releases the lock at the holder's commit time. The
       committer's subsequent snapshot hydration SELECT (Step 3) takes a
       FRESH snapshot at the SELECT's execution time, which is AFTER the
       holder's commit — so it observes the holder's committed work.
       Stricter isolation (REPEATABLE READ, SERIALIZABLE) would freeze the
       snapshot at tx start and the hydration SELECT would NOT see the
       upstream committer's rows. This silently breaks causal ordering and
       produces spurious InsufficientInventory errors at low inventory
       levels. The implementation MUST verify the tx runs at READ COMMITTED.)

      NOTE ON TRANSACTION TOPOLOGY. Each SuperBatch is processed in its own
      top-level transaction (StartTransactionCommand to CommitTransaction
      Command). This gives clean semantics:
      - One WAL fsync per SuperBatch (B1 amortization assumption holds).
      - committer_tx_id is a top-level XID; pg_xact_status returns its
        actual final state (committed/aborted/in_progress), unambiguously
        queryable by orphan-recovery (§3.2).
      - Cost rows are durable at CommitTransactionCommand return; subsequent
        committer crashes leave durable cost rows intact, enabling the
        post-commit-pre-cleanup recovery path (§3.2 committed branch).
      An earlier draft used BeginInternalSubTransaction inside a long-lived
      BGWorker parent transaction. That model was unsuitable: subtransaction
      release does not flush WAL, the parent tx would accumulate XIDs
      indefinitely (xmin horizon bloat, wraparound risk), and on BGWorker
      crash the postmaster would abort the parent and wipe all released
      subtransactions' work. Top-level-per-SuperBatch avoids all three.

      committer_tx_id = pg_current_xact_id_if_assigned() (force allocation via
                                                          one-row temp insert).
      Atomically: store committer_tx_id in CommitterQueueEntry.

      LAZY LOCK-ROW CREATION — DETERMINISTIC LOOP. PostgreSQL does not
      guarantee the row-acquisition order of `INSERT ... UNNEST(...) ON
      CONFLICT DO NOTHING` against concurrent writers. UNNEST emits rows
      in array order but the executor may write them in any order, and
      tuple-level exclusive locks are taken as rows are written. If two
      committers concurrently INSERT overlapping pool_key sets, they can
      deadlock during the bulk INSERT phase, before either reaches the
      FOR UPDATE singleton loop. Lazy creation MUST therefore use the
      same singleton-loop pattern as FOR UPDATE acquisition, in sorted
      lex order:

        for (sku_id, location_id) in sorted_sku_keys:
          SPI: INSERT INTO poc_v21_pool_locks (sku_id, location_id)
                 VALUES ($1, $2)
               ON CONFLICT DO NOTHING.

        for (work_order_id, operation_id) in sorted_wip_keys:
          SPI: INSERT INTO poc_v21_wip_pool_locks
                 (work_order_id, operation_id)
                 VALUES ($1, $2)
               ON CONFLICT DO NOTHING.

      Each call is independent at the SPI level; PG cannot reorder across
      SPI boundaries. The same prepared-statement requirement applies as
      for FOR UPDATE — without preparation, the per-call cost is ~50μs;
      with preparation, ~5-10μs.

      Eager creation at SKU/WO setup time is a viable alternative that
      moves this cost off the hot path entirely; see §7 Q-C for the
      open question. PoC implements lazy creation with singleton-loop.

      DETERMINISTIC LOCK ACQUISITION. PostgreSQL does NOT guarantee that
      `SELECT ... ORDER BY ... FOR UPDATE` locks rows in ORDER BY order —
      the planner may lock rows during an index or sequential scan BEFORE
      applying the sort node. For cross-SuperBatch deadlock avoidance under
      the affinity-grouping split fallback (where two committers process
      chunks of the same connected component and overlap on pool_keys), the
      lock acquisition order MUST be ironclad-deterministic. We do this by
      issuing singleton FOR UPDATE statements in a tight Rust loop in
      sorted lex order:

        for (sku_id, location_id) in sorted_sku_keys:
          SPI: SELECT 1 FROM poc_v21_pool_locks
                 WHERE sku_id = $1 AND location_id = $2 FOR UPDATE.

        for (work_order_id, operation_id) in sorted_wip_keys:
          SPI: SELECT 1 FROM poc_v21_wip_pool_locks
                 WHERE work_order_id = $1 AND operation_id = $2 FOR UPDATE.

      Each SPI call locks exactly one row. PG cannot reorder across SPI
      boundaries. Cost: one SPI call per deduplicated pool_key in the
      SuperBatch. With prepared statements (mandatory — see below), the
      per-call cost is ~5-10μs. For a SuperBatch with 100 deduped pools,
      total ~0.5-1ms across both phases.

      PREPARED STATEMENT REQUIREMENT. Both singleton-loop queries (lazy
      INSERT and FOR UPDATE, four total) MUST be prepared once per
      committer (e.g., during committer initialization) and reused. A
      non-prepared SPI call re-parses and re-plans the query on each
      invocation, raising per-call cost to ~50μs. Prepared statements
      keep the total cost in the budget.
 8. STEP 2.5 (dedup):
      Two dedup paths run in parallel; either hit means the event was
      previously processed and should be replayed from existing rows.

      (a) Consumption-side dedup. Collect all (issue_id, method_used) pairs
          from the batch's consumption events.
          SPI: SELECT (issue_id, method_used) FROM poc_v21_cost_depletions WHERE
                (issue_id, method_used) IN (UNNEST ...)
                UNION
                SELECT (issue_id, method_used) FROM poc_v21_cost_consumptions WHERE
                (issue_id, method_used) IN (UNNEST ...).

      (b) Receipt-side dedup. Collect all correlation_ids from the batch's
          receipt events (PoReceipt, WoComplete output, InvAdjust with
          positive qty). Layers don't carry issue_id; their natural dedup
          key is correlation_id (one envelope produces its layers exactly
          once).
          SPI: SELECT correlation_id FROM poc_v21_cost_layers WHERE
                correlation_id IN (UNNEST ...).

      Partition events into replayed_events (hit by either path) and
      to_plan (no hit). For replayed_events: read existing rows by the
      matching key to reconstruct the result for that envelope. The
      replay path emits no new rows; the result vectors carry the
      already-persisted row IDs for the caller's status update.
 9. STEP 3 (snapshot):
      SPI: SELECT FROM poc_v21_cost_layers WHERE (sku_id, location_id) IN
            (UNNEST ...) AND effective_qty > 0 with ROW_NUMBER per pool ≤ 1000.
            (For FIFO method state.)
      SPI: SELECT sku_id, location_id, avg_unit_cost, avg_total_qty
             FROM poc_v21_avg_pool_state
             WHERE (sku_id, location_id) IN (UNNEST ...).
            (Incrementally-maintained running average; Step 5 UPSERTs this
             table to keep it current. NEVER reconstruct AVG from cost_layers
             history — incremental maintenance is the contract.)
      SPI: SELECT DISTINCT ON (sku_id, location_id)
                 sku_id, location_id, unit_cost
             FROM poc_v21_standard_costs
             WHERE (sku_id, location_id) IN (UNNEST ...)
               AND effective_from <= NOW()
             ORDER BY sku_id, location_id, effective_from DESC.
            (Latest standard cost effective as of NOW(). DISTINCT ON +
             ORDER BY ... DESC gives the most-recent effective_from per
             pool.)
      SPI: SELECT sku_id, location_id, MAX(born_seq)
             FROM poc_v21_cost_layers
             WHERE (sku_id, location_id) IN (UNNEST ...)
             GROUP BY sku_id, location_id;
           SELECT layer_id, MAX(consumed_seq)
             FROM poc_v21_cost_depletions
             WHERE layer_id IN (...)
             GROUP BY layer_id.
            (Seed local sequence generators. NOTE: GROUP BY omits keys with
             no matching rows; for a brand-new pool with no prior layers,
             the result set will not contain that (sku_id, location_id)
             pair at all. The Rust hydration code MUST initialize the
             per-pool sequence generator at 0 when a pool_key is absent
             from the result set — the absence of a row means the pool
             has no prior layers, and 0 is the correct starting sequence
             number. Same rule applies to consumed_seq for newly-created
             layers in this SuperBatch.)
      Build PocV21Snapshot.
10. STEP 4 (dispatch):
      Sort all to_plan events by (business_date, doc_chrono, document_id,
      sub_priority, request_seq, event_seq).
      (This is the chronological ordering used throughout acct. The first
       four keys come from the envelope's payload. The last two are
       tiebreakers from staging-side metadata: request_seq is the staging
       queue's monotonic counter assigned at enqueue time;
       event_seq (field on PocV21Event) is a 0-based index assigned when
       constructing the envelope's event list. Together these guarantee a
       total order on events that exists pre-INSERT — posting_line_id is
       assigned by Step 5's INSERT and cannot serve as a sort key here.
       Within a SuperBatch, events from multiple envelopes that touch the
       same pool are interleaved by chrono order. The shared per-pool
       snapshot mutates as events apply, so a consume-from-X event run
       after a receipt-into-X event in the same SuperBatch sees the layer
       the receipt just created. This is how the SuperBatch handles overlap
       correctly: events from overlapping envelopes are merged into one
       chronologically-ordered stream and applied against shared snapshots.)
      Initialize empty result vectors.
      For each event (in the sorted order above):
        method = dispatch_event(event, snapshot).
        result = method.plan_apply(&[event], snapshot).
        (plan_apply is called with a single-event slice. The shared snapshot
         carries the cumulative state across all prior events; a method
         sees the current state of every pool it touches. Methods that
         need multi-event context — e.g., FIFO consuming multiple layers
         to satisfy one event's qty — handle that internally; they don't
         need a multi-event input.)
        If result has error: record envelope-level error: mark this event's
                              envelope as failed. Roll back any in-memory
                              snapshot mutations made by this event AND any
                              prior events from the same envelope (the
                              envelope's contribution is reverted atomically).
        Else: append result rows to result vectors. Snapshot already mutated
              by plan_apply.
      Filter result vectors to include only committed-envelope rows (events
      from failed envelopes are excluded).
11. STEP 5 (bulk insert):
      SPI: INSERT INTO poc_v21_posting_lines ... UNNEST(...).
      SPI: INSERT INTO poc_v21_cost_layers ... UNNEST(...).
      SPI: INSERT INTO poc_v21_cost_depletions ... UNNEST(...).
      SPI: INSERT INTO poc_v21_cost_consumptions ... UNNEST(...).
      SPI: INSERT INTO poc_v21_posting_line_inventory ... UNNEST(...).
      SPI: INSERT INTO poc_v21_avg_pool_state ... UNNEST(...)
             ON CONFLICT (sku_id, location_id) DO UPDATE SET
               avg_unit_cost = EXCLUDED.avg_unit_cost,
               avg_total_qty = EXCLUDED.avg_total_qty,
               last_updated_at = EXCLUDED.last_updated_at,
               last_committer_tx_id = EXCLUDED.last_committer_tx_id.
            (Only for AVG-method pools touched by this batch. Plan_apply
             computed the new running average in-memory; this UPSERT
             persists it. The lex-lock on pool_locks ensures no concurrent
             committer modifies the same pool, so the running average
             is race-free.

             CRITICAL: the UNNEST input arrays MUST contain exactly one
             entry per (sku_id, location_id). If the committer's array
             builder appends snapshot state per-event, a pool touched by
             N events ends up in the array N times, and PG aborts the
             SuperBatch with `ERROR: ON CONFLICT DO UPDATE command cannot
             affect row a second time`. The committer must collect the
             FINAL snapshot state for each pool keyed by (sku_id,
             location_id) — typically via a HashMap<(i64, i64),
             SkuPoolState> populated during Step 4 — and emit one UNNEST
             row per distinct key.)
      IF any envelope in the SuperBatch was submitted with durable_queue=true
      (and poc_v21.persistent_staging=on, which is required by the validation
       gate at enqueue time):
        SPI: UPDATE poc_v21_persistent_staging
                SET state='completed'
              WHERE correlation_id = ANY($1::uuid[]).
             (Only for the correlation_ids whose envelopes were durable_queue=true.
              Transitions the persistent-staging row directly from 'staged' (the
              caller-set initial state) to 'completed', skipping the 'in_shmem'
              state entirely on the hot path. The 'in_shmem' state is reserved
              for recovery-sweep diagnostics (§3.6). Skipped entirely if no
              durable envelopes were in this SuperBatch.)
      (6 UNNEST statements per SuperBatch in the standard case; 7 when at
       least one durable envelope is present. The base figure 6 is the
       relevant baseline for the bulk-UNNEST efficiency criterion P5.)
12. Status writes (UPSERT — required for committer_lazy mode correctness):

      Each transition uses INSERT ... ON CONFLICT (correlation_id) DO UPDATE
      rather than plain UPDATE. This is REQUIRED for `committer_lazy` mode,
      where the caller's enqueue does NOT INSERT the initial 'queued' row;
      a plain UPDATE would match zero rows and silently lose the terminal
      state, leaving pollers hanging indefinitely. Under `caller_intx` mode
      the row already exists at enqueue time and the UPSERT's ON CONFLICT
      branch fires; the cost difference vs plain UPDATE is microseconds of
      conflict-check work inside PG's executor — well below the per-SPI
      noise floor. Universal UPSERT is used regardless of status_insert_mode
      for a single code path.

      SPI: INSERT INTO poc_v21_submission_status
             (correlation_id, state, enqueued_at, committed_at,
              committer_tx_id, superbatch_id)
             SELECT u.id, 'committed', u.enqueued_at, now(), $1, $2
               FROM UNNEST($3::uuid[], $4::timestamptz[]) AS u(id, enqueued_at)
           ON CONFLICT (correlation_id) DO UPDATE SET
             state = EXCLUDED.state,
             committed_at = EXCLUDED.committed_at,
             committer_tx_id = EXCLUDED.committer_tx_id,
             superbatch_id = EXCLUDED.superbatch_id.
           (Successful commits. Bound arrays carry per-envelope correlation_id
            and enqueued_at_micros from staging. committer_tx_id and
            superbatch_id are scalars — same for every envelope in the batch.
            ON CONFLICT preserves the existing enqueued_at; the value bound
            for the INSERT path only matters for committer_lazy mode, where
            the row is being created here for the first time.)

      SPI: INSERT INTO poc_v21_submission_status
             (correlation_id, state, enqueued_at, processed_at,
              error_code, error_detail)
             SELECT u.id, 'failed', u.enqueued_at, now(), u.err_code, u.err_detail
               FROM UNNEST($1::uuid[], $2::timestamptz[], $3::text[], $4::jsonb[])
               AS u(id, enqueued_at, err_code, err_detail)
           ON CONFLICT (correlation_id) DO UPDATE SET
             state = EXCLUDED.state,
             processed_at = EXCLUDED.processed_at,
             error_code = EXCLUDED.error_code,
             error_detail = EXCLUDED.error_detail.
           (Failed envelopes. Per-envelope error_code and error_detail.)

      SPI: INSERT INTO poc_v21_submission_status
             (correlation_id, state, enqueued_at, processed_at,
              committer_tx_id)
             SELECT u.id, 'replayed', u.enqueued_at, now(), $1
               FROM UNNEST($2::uuid[], $3::timestamptz[]) AS u(id, enqueued_at)
           ON CONFLICT (correlation_id) DO UPDATE SET
             state = EXCLUDED.state,
             processed_at = EXCLUDED.processed_at,
             committer_tx_id = EXCLUDED.committer_tx_id.
           (Replayed envelopes — dedup-lookup hit, no new rows written.)

      SPI count: 3 statements per SuperBatch (same as the prior UPDATE-only
      design). Per-SuperBatch overhead is unchanged.
13. CommitTransactionCommand. (Single WAL fsync for entire SuperBatch. Cost
    rows are durable at this point; subsequent committer crash leaves cost
    rows intact.)
14. Post-commit cleanup:
      For each staging entry in the original SuperBatch (NOT the filtered-by-
      ejection batch — iterate ALL indices that were assigned to this
      SuperBatch by the router):
        Attempt CAS staging.valid: 3 → 0 (empty).
        IF CAS 3→0 SUCCEEDS:
          Free this entry's spillover-arena blocks (payload, pool_keys).
          Continue to next entry.
        IF CAS 3→0 FAILS:
          Read staging.valid to determine current state:
            valid == 1 (pending): this entry was EJECTED during Step 4
              (committer found user_tx_xid in_progress and CAS'd valid
              3 → 1). The entry now belongs to the router again; its
              arena blocks (payload, pool_keys) are still in use because
              the router will re-pick it. DO NOT free its arena blocks.
              Skip and continue.
            valid == 2 (processing): the router died mid-stamp before
              transitioning this entry to valid=3 (router's Phase 6 step
              (j) didn't complete for this entry). The committer
              successfully processed the SuperBatch's other entries that
              WERE stamped to valid=3, AND this entry's user_tx_xid /
              payload were already read at Step 4 (the committer processed
              this envelope regardless of valid state). The committer's
              cost rows are durable; this entry's slot needs reclaiming.
              CAS staging.valid: 2 → 0 (empty). On success: free the
              spillover-arena blocks. On failure (e.g., the router-recovery
              sweep already reclaimed it concurrently): skip — already
              reclaimed.
            valid == 0 (empty): another cleanup path already handled this
              entry (e.g., router-recovery sweep). Skip.
            valid == 3 (routed): impossible if the original CAS 3→0 failed
              — re-read race. Retry once; if still fails, treat as race
              with concurrent cleanup and skip.
      Signal staging-queue backpressure CV.
      CAS CommitterQueueEntry.valid: 2 → 3 (completed). On next committer
      tick, CAS 3 → 0 to free the queue slot.

      The CAS-failure-with-state-check semantics on staging cleanup is the
      load-bearing rule for slot lifecycle correctness. Three cases are
      handled distinctly:
      - Normal completion (CAS 3→0 success).
      - Ejection during Step 4 (CAS fails, valid==1, skip — router owns).
      - Router-died-mid-stamp (CAS fails, valid==2, CAS 2→0 fallback).
      Conflating any two of these causes slot leaks or arena double-frees.
```

### 1.9 Durability semantics: the `durable_queue` parameter

The enqueue function accepts a `durable_queue: bool` parameter (default `false`). It controls whether the staging-queue write survives a postmaster crash that occurs between enqueue success and committer processing.

**Two implementations behind one switch:**

- **`durable_queue = false` (default)**: shmem-only staging. The enqueue write is a non-WAL shmem operation. If postmaster crashes after the function returns but before the committer commits the cost rows, the envelope is lost. The status row in `poc_v21_submission_status` (if it exists under `caller_intx` mode) will be transitioned to `state='failed', error_code='postmaster_restart_loss'` by the §3.6 recovery sweep. The caller can observe this terminal state by polling `poc_v21_submission_status`.
- **`durable_queue = true`**: persistent staging. In addition to the shmem write, the enqueue function INSERTs a row into `poc_v21_persistent_staging` within the caller's user-tx. The row is WAL-logged and survives postmaster restart. On restart, the §3.6 recovery sweep replays rows from `poc_v21_persistent_staging` back into the shmem staging queue. Envelopes that hadn't yet been committed by the committer are re-routed and processed normally.

**Function signature:**

```sql
SELECT poc_v21_enqueue(
    correlation_id := $1,    -- UUID
    event_type     := $2,    -- text
    payload        := $3,    -- jsonb
    pool_keys      := $4,    -- jsonb
    durable_queue  := $5     -- boolean, default false
);
-- Returns void on success.
-- Raises ERRCODE_INSUFFICIENT_RESOURCES on staging queue full + timeout.
-- Raises ERRCODE_FEATURE_NOT_SUPPORTED if durable_queue=true but
--   poc_v21.persistent_staging GUC is off (compile-time or runtime disabled).
```

The function NEVER silently downgrades durability. If the caller requests `durable_queue=true` on a deployment without persistent staging available, the function errors. Callers can rely on the parameter meaning what it says: success implies the requested durability was actually delivered.

**Persistent staging table:**

```sql
CREATE TABLE poc_v21_persistent_staging (
    request_seq      BIGSERIAL PRIMARY KEY,
    correlation_id   UUID NOT NULL UNIQUE,
    enqueued_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_tx_xid      xid8 NOT NULL,
    event_type       TEXT NOT NULL,
    payload          JSONB NOT NULL,
    sku_pool_keys    JSONB NOT NULL,           -- array of (sku_id, location_id)
    wip_pool_keys    JSONB,                    -- nullable; only for wo_complete-style events
    business_date    DATE NOT NULL,
    state            TEXT NOT NULL CHECK (state IN ('staged', 'in_shmem', 'completed'))
                     DEFAULT 'staged'
);
CREATE INDEX poc_v21_persistent_staging_state
  ON poc_v21_persistent_staging (state, enqueued_at)
  WHERE state IN ('staged', 'in_shmem');
```

The `state` field tracks the row's lifecycle:
- `staged` — INSERTed by the caller's enqueue path under `durable_queue=true`. Default initial state. Persists from caller commit until committer's Step 5 transitions it to `completed`.
- `in_shmem` — set ONLY by the postmaster-restart recovery sweep when it replays a previously-staged envelope back into shmem after restart. Distinguishes "originally staged, restored after restart" from "still in original staged state" for diagnostic purposes. Hot-path committer processing does NOT pass through this state; bundling a per-envelope UPDATE on the committer claim path would destroy batching efficiency.
- `completed` — committer committed the cost rows; this persistent row may be GC'd.

**Enqueue flow with `durable_queue=true`:**

```
 1. Construct envelope (same as durable_queue=false).
 2. Force user_tx_xid allocation.
 3. INSERT row into poc_v21_persistent_staging with state='staged'. (Within
    caller's user-tx; WAL-logged.)
 4. Write StagingEntry to shmem. CAS valid: 0 → 1 (pending).
    NOTE: The committer's processing must wait for caller's user-tx to commit
    before treating the envelope as live (per §3.4 caller-tx coupling).
 5. Caller's user-tx commits (or aborts). On commit, the persistent_staging
    row is durable AND the shmem entry is observable.
```

**Committer flow under persistent staging:**

The committer does NOT transition `staged → in_shmem` on the hot path — that would require a per-envelope UPDATE before the top-level transaction begins, defeating the SuperBatch's batching efficiency. Instead, the committer transitions `staged → completed` (skipping `in_shmem` entirely) as part of Step 5's bulk write, batched across all durable correlation_ids in the SuperBatch:

```sql
UPDATE poc_v21_persistent_staging
   SET state='completed'
 WHERE correlation_id = ANY($1::uuid[]).
```

This is the 7th UNNEST/UPDATE statement in Step 5 when at least one envelope in the SuperBatch was submitted with `durable_queue=true`; it is skipped entirely when no durable envelopes are present. The UPDATE runs inside the committer's top-level transaction, so it commits atomically with the cost rows. A periodic GC job purges `completed` rows older than a retention threshold (default 24 hours; not in scope of PoC must-pass but measured).

The `in_shmem` state is reserved for the recovery sweep (§3.6) to distinguish "this row was originally staged and has been replayed back to shmem after a restart" from "this row has never seen a successful router pickup." For hot-path operation, the row goes directly `staged → completed`.

**Postmaster restart recovery with persistent staging:**

The startup recovery worker scans `poc_v21_persistent_staging WHERE state IN ('staged', 'in_shmem')`:

- For each row: verify the user_tx_xid status in pg_xact.
  - `committed`: the caller commit happened durably. Re-INSERT the envelope into shmem staging queue with CAS valid: 0 → 1. Set persistent row state to `in_shmem`.
  - `aborted`: the caller's user-tx rolled back. Persistent row is orphaned; delete it. Under `caller_intx` mode, the submission_status row rolled back with the user-tx (no row exists). The committer's lazy fallback path will create a `state='failed', error_code='caller_tx_aborted'` row when it next encounters the staging entry — but since we just deleted the persistent row, the envelope won't be re-routed. Under `committer_lazy + persistent_staging`, the lazy committer-side INSERT applies directly during recovery (INSERT a `state='failed', error_code='caller_tx_aborted'` row via `ON CONFLICT (correlation_id) DO NOTHING`).
  - `in_progress`: shouldn't happen (postmaster restart kills all backends; their tx's transition to aborted). Defensive: treat as aborted.
- Cost rows existing for the correlation_id (`SELECT 1 FROM cost_layers WHERE correlation_id = $1 LIMIT 1`) means the committer committed before the crash. Set persistent row to `completed`, set status row to `committed`. No re-enqueue.

After this sweep, the system resumes normal operation. Envelopes submitted with `durable_queue=true` are recovered; envelopes submitted with `durable_queue=false` whose committers hadn't yet committed are marked failed.

**Cost of durability:**

Per-enqueue:
- Additional INSERT into persistent_staging within the caller's user-tx.
- The caller's user-tx commit WAL-logs that INSERT (no extra fsync — rides with the caller's normal commit).
- One extra UPDATE per envelope at committer's Step 5 (transition to `completed`).

Throughput delta vs `durable_queue=false`: measured in §5.5. Expected: 5-30% per-enqueue overhead depending on row size; minimal impact on committer pipeline.

**Caller guidance (informational, not enforced):**

The PoC does not enforce when callers should use `durable_queue=true`. The pattern is workload-dependent:

- **Batch operator-driven workflows** (WO completions, EOD posting, bulk receipts): `durable_queue=false` is typically appropriate. The operator can monitor progress via `poc_v21_submission_status` polling and re-submit failed envelopes. The throughput benefit is meaningful at scale.
- **Singular high-stakes interactions** (manual journal entries, financial-period closing postings, anything where the caller needs an authoritative success/failure outcome before walking away): `durable_queue=true`. After enqueue, the caller polls `poc_v21_submission_status` for terminal state. A postmaster restart mid-polling doesn't lose the work; the recovery sweep re-enqueues it and the poll eventually sees the outcome.

The application tier may build a `ledger_enqueue_and_wait(...)` helper that combines `durable_queue=true` with a polling loop on `poc_v21_submission_status`. That helper is application-layer code, not extension machinery. The PoC does NOT implement it; the bake-off measures the underlying primitives.

### 1.10 What the PoC explicitly does NOT do

- No WAC methods. No close-hook DAG. No variance routing. No provisional flagging.
- No real BOM expansion. The test harness generates synthetic `wo_complete` payloads with random component counts.
- No multi-currency. Single currency throughout; the schema omits currency columns.
- No lots, units, or other identity dimensions.
- No webhook delivery. Status observable via `poc_v21_submission_status` polling only.
- No XactCallback-based user-tx coupling (Option A) or fire-and-forget (Option B). Only pg_xact-check Option (C).
- No replica/HA story. Single primary.
- No built-in sync-wait mechanism on enqueue. Callers wanting synchronous-final-outcome poll `poc_v21_submission_status` after enqueue.
- No GC of completed persistent_staging rows in primary PoC scope (mentioned as 24hr retention but not exercised).

---

## 2. Theoretical Upper Bounds

Established analytically before measurement. If measured numbers fall far below, the gap localizes where the architecture loses efficiency.

### 2.1 Bottlenecks, in order of expected impact

**B1: WAL fsync per committer-tx commit (GLOBAL, not per-shard).** Each SuperBatch produces one COMMIT and one fsync (assuming `synchronous_commit=on`). v2.1's central performance claim is that batching N envelopes into one SuperBatch reduces fsyncs by ~N×.

For NVMe with `fdatasync` at ~10K commits/sec global ceiling:
- SuperBatch of 50 envelopes × 1 event each (simple inv_adjust workload) = 500K events/sec WAL-ceiling.
- SuperBatch of 50 envelopes × 5 events each (wo_complete K=5) = 2.5M events/sec WAL-ceiling.
- SuperBatch of 1 envelope (hot-pool degenerate) × 5 events = 50K events/sec WAL-ceiling.

The batch_size_max GUC is the primary lever for B1.

**B2: SPI overhead per SuperBatch.** Each SuperBatch performs (in the no-replay, no-error case):
- **P singleton INSERT-ON-CONFLICT-DO-NOTHING on pool_locks** for lazy creation, where P is the count of deduplicated SKU pool_keys. Singleton-loop is required to avoid bulk-INSERT tuple-lock deadlocks per §1.8 Step 2.
- **Q singleton INSERT-ON-CONFLICT-DO-NOTHING on wip_pool_locks** for lazy creation, where Q is the count of deduplicated WIP pool_keys.
- **P singleton SELECT FOR UPDATE on pool_locks** for deterministic lock acquisition per §1.8 Step 2.
- **Q singleton SELECT FOR UPDATE on wip_pool_locks** for deterministic lock acquisition.
- 1 SELECT for consumption-side dedup-lookup (issue_id × method_used pairs).
- 1 SELECT for receipt-side dedup-lookup (correlation_ids in cost_layers).
- 1 SELECT for layer hydration (with ROW_NUMBER per pool).
- 1 SELECT for AVG pool state.
- 1 SELECT for STD lookup.
- 1 SELECT for sequence-seed (MAX born_seq) hydration.
- 6 INSERT/UPSERT statements for cost rows (posting_lines, cost_layers, cost_depletions, cost_consumptions, posting_line_inventory, avg_pool_state) — each an UNNEST or UNNEST+ON CONFLICT.
- 1 UPDATE for persistent_staging completed-transition (only when SuperBatch contains durable-queue envelopes; conditional).
- 1-3 INSERT-ON-CONFLICT-DO-UPDATE statements for status writes (committed, failed, replayed groups). UPSERT replaces plain UPDATE so the same statements correctly handle `committer_lazy` mode (where no row exists at Step 12 entry). Per-statement cost is essentially identical to plain UPDATE when the row exists (ON CONFLICT branch fires); the INSERT branch incurs a few microseconds of internal PG executor work, well below the SPI per-call floor.

Fixed SPI calls: ~12. Variable SPI calls: 2(P + Q) (singleton-loop INSERT-ON-CONFLICT + singleton-loop FOR UPDATE for both lock domains). For a SuperBatch with batch_size_max=50 wo_complete envelopes (K=5 components + WIP + output = 7 pool_keys each), deduplicated pool_keys ≈ 50-100 (depends on overlap density). So 2(P + Q) ≈ 100-200.

Total: ~110-210 SPI calls per SuperBatch. With prepared statements (per the §1.8 Step 2 requirement), singleton-loop SPI is ~5-10μs each (~1-2ms total). Fixed SPI calls remain ~10-50μs each (~120-600μs total). Per-SuperBatch SPI budget: 1.1-2.6ms. Per-committer ceiling: ~400-900 SuperBatches/sec when SPI-bound.

With SuperBatch of 50 envelopes × 5 events = 250 events per batch: per-committer ~100K-225K events/sec. Aggregate scales with committer count, capped by B1 globally.

**The doubled singleton-loop cost is the price of deterministic lock-row creation AND lock acquisition** (see §1.8 Step 2). The previous design (bulk UNNEST INSERT + UNNEST ORDER BY FOR UPDATE) was both vulnerable to lazy-creation deadlocks during the INSERT phase AND non-deterministic in lock-acquisition order during the FOR UPDATE phase — two distinct deadlock surfaces. Eager creation at SKU/WO setup time is a viable optimization that moves the lazy-INSERT cost off the hot path entirely; see §7 Q-C.

**B3: Router throughput.** Single BGWorker, no SPI in hot path. Each router tick performs **affinity grouping** over the window: union-find over envelopes connected by shared pool_keys.

Per-tick work:
- Window read: 1000 envelopes × atomic load = ~50μs.
- Build pool_key → envelope_idx map: 1000 envelopes × ~17 pool_keys each = 17K hashmap insertions ≈ 850μs (~50ns per insert under fast hashers).
- Union-find unions: for each pool_key with ≥2 envelopes, union them. Bounded by total pool_key occurrences = 17K operations × ~50ns = 850μs.
- Collect components and sort by min request_seq: O(W log G) where G is the number of components. For W=1000 and ~100 components: ~7K compares ≈ 7μs.
- Per envelope: CAS staging.valid 1→2 (~20ns) + per-envelope publication (~50ns).
- Per-SuperBatch lock-set dedup union sort: SuperBatch has up to batch_size_max=50 envelopes × ~17 pool_keys = 850 keys, sort-and-dedup ~5μs per SuperBatch.

Total per-tick cost: ~2-3ms for a full window. Router ceiling: ~300-500 ticks/sec at full window saturation. At 1000 envelopes per tick that's ~300K-500K envelopes/sec NOT bounded by routing (the window read is the dominant cost; the grouping is incidental at this scale).

Realistic operating point: workloads with smaller windows (200-400 envelopes per tick under moderate load) and proportionally smaller per-tick cost. At 200 envelopes/tick the grouping cost drops to ~400μs/tick, ticking at 2500/sec for ~500K envelopes/sec.

The router has been moved from "binding constraint at low ticks/sec" (the previous incorrect FIFO design's claim) to a real but generous ceiling that affinity grouping costs do not push below the committer's WAL ceiling for any reasonable workload. The router is NOT the binding constraint.

**B4: Router-bound saturation.** If callers produce more than the router can pack, staging queue fills. Backpressure kicks in via queue_full_timeout. Callers wait or error. This is correct behavior under sustained overload but caps observable throughput.

**B5: Lock acquisition contention across SuperBatches.** v2.1's affinity grouping routes overlapping work into one SuperBatch — one committer handles the contention via in-memory snapshot mutation, NO inter-committer FOR UPDATE waits within a single contention group. Cross-SuperBatch contention only arises when:

1. A single connected component (envelopes transitively sharing pool_keys) exceeds `batch_size_max` and is split into multiple chunks. The chunks run as separate SuperBatches; their committers serialize via PG's row locks on `pool_locks`.
2. Affinity boundaries shift between router ticks. If Env_A is routed in tick N, then Env_B arrives in tick N+1 sharing a pool_key with Env_A's already-processing SuperBatch, Env_B's SuperBatch will FOR UPDATE-block on Env_A's committer until Env_A commits. This is the temporal-boundary case: affinity is per-tick.

Both are bounded fallbacks, not the hot path. The architecture's expected operating regime is: most contention groups fit within batch_size_max and are routed together by affinity grouping; cross-SuperBatch FOR UPDATE serves as the safety net for genuine overflow.

Worst case for cross-SuperBatch contention: a workload where a single contention group consistently exceeds batch_size_max (e.g., 200 envelopes/router-tick all touching SKU 5). Under default batch_size_max=50, this splits into 4 SuperBatches that serialize through one committer at a time. Aggregate throughput on the overlapping pool degrades to single-committer rate (~1-6K SuperBatches/sec from B2 × 50 envelopes/SuperBatch = 50K-300K envelopes/sec on the contended pool). Other committers remain available for non-overlapping work in independent SuperBatches.

**B6: Waiter wake latency.** Caller backends use synchronous-enqueue; they wait on the backpressure CV when queue is full. Wake latency ~10μs per backend. Not a throughput cap, but contributes to p99 latency under sustained pressure.

### 2.2 Combined theoretical ceiling

For the bake-off's primary workload (mixed inv_adjust + wo_complete K=5, disjoint pools, sync_commit=on, batch_size_max=50):

- B1 (WAL fsync) ceiling: ~2.5M events/sec (50 envelopes × 5 events × 10K commits/sec).
- B2 (SPI) ceiling: ~1M events/sec across 4 committers (each ~250K events/sec).
- B3 (router) ceiling: ~300K-500K envelopes/sec ≈ 1.5M-2.5M events/sec.
- Realistic combined: 100-500K events/sec aggregate, capped by interactions between B1 (WAL fsync) and B2 (committer CPU/SPI). Router is no longer a binding constraint for any reasonable workload.

The bottleneck classification (§5.7) records which bottleneck binds at each cell of the bake-off matrix.

---

## 3. Failure Modes

Each failure mode has trigger, detection, recovery, test, status (must-pass vs best-effort).

### 3.1 Committer-tx failure

**Trigger:** committer's top-level transaction raises during Step 5 INSERTs (constraint violation, deadlock with non-committer code, OOM in plan cache, etc.).

**Detection:** pgrx's error-catching wrapper (e.g., PgTryBuilder) catches the ERROR before CommitTransactionCommand. The transaction is rolled back via AbortCurrentTransaction.

**Recovery:**
- Transaction aborts. Locks release. No rows persist (no fsync happened).
- All envelopes in batch get `state='failed'`, `error_code='committer_tx_failure'`, with the specific PG error code in `error_detail`. (Status writes happen in a fresh top-level transaction immediately following the failed one.)
- CAS staging entries: 3 → 0 (empty); free spillover blocks.
- CAS CommitterQueueEntry: 2 → 3 (completed).
- Next committer continues normally.

**Test:** `test_v21_committer_tx_constraint_violation` injects a failure via test-only error-injection point inside the INSERT path. Assert: all envelopes failed; no rows persisted; next SuperBatch succeeds.

**Status:** PoC-must-pass.

### 3.2 Committer death

**Trigger:** committer backend SIGKILL between Step 2 lock acquisition and Step 5 commit.

**Detection:** committer's BGWorker slot+generation in CommitterQueueEntry no longer matches a live worker, AND `committer_acquired_at_ns` is stale. Next active committer scanning the queue:
- If `now - committer_acquired_at_ns > committer_lease_ms` AND the stored `(committer_bgw_slot, committer_bgw_generation)` no longer identifies a live worker (the slot is empty OR the generation has advanced) → stale.
- CAS-attempt to claim: replace the stale (slot, generation) with the contender's own (slot, generation).

**Recovery:**
- Read dead committer's `committer_tx_id` from queue entry.
- Query pg_xact for that tx's status.
  - `committed`: work is durable; the dead committer's top-level transaction committed cost rows but may not have completed its Step 14 staging cleanup. The takeover committer runs the Step 14 cleanup logic on the dead committer's behalf: for each linked staging entry, attempt CAS valid: 3 → 0 (some entries may already be at 0 if the dead committer partially completed cleanup; skip those). Free arena blocks for each entry transitioned to 0. Free the CommitterQueueEntry's OWN arena blocks (staging_entry_offsets array, sorted pool_keys arrays — queue-entry-owned, not staging-entry-owned). Update submission_status rows to 'committed' (idempotent — likely already done by the dead committer pre-death; uses `... ON CONFLICT DO NOTHING` or detects existing 'committed' state and no-ops). CAS queue entry: 2 → 3 (completed), then 3 → 0 on the takeover committer's next tick.
  - `aborted`: work is gone. CAS queue entry valid: 2 → 1 (ready) for re-processing. Re-claim and re-execute Steps 1-5.
  - `in_progress`: poll with exponential backoff up to `committer_lease_ms × 10`. After bound, treat as aborted. (Note: this poll is for the DEAD committer's committer_tx_id, not for a live caller's user_tx_xid. The "committer never sleeps for callers" rule from §3.11 applies to caller-tx checks in Step 4; orphan recovery for a dead committer is a one-shot recovery path, not a hot-path concern.)

**Test:** `test_v21_committer_sigkill_pre_commit` and `test_v21_committer_sigkill_post_commit`. Harness spawns helper backend that enters committer pipeline, then SIGKILLs at injected point. Assert:
- Pre-commit kill: SuperBatch is re-processed; cost rows reflect only one execution; staging slots eventually free.
- Post-commit kill: status is updated correctly; cost rows are not duplicated (idempotent via dedup-lookup on retry, or via post-commit recovery path); staging slots eventually free.
- Lease takeover within 2× committer_lease_ms.

**Test (concurrent recovery race):** `test_v21_committer_orphan_double_claim_race`. Kill a committer mid-execution. Start 4 fresh committers simultaneously, each scanning the queue. Each detects the stale lease and attempts CAS-claim. Assert: exactly one committer wins the CAS; the others detect the loss and proceed to other work. The winning committer correctly re-executes Step 1-5; cost rows are not duplicated (dedup-lookup catches any partially-committed rows from the dead committer); staging slots eventually free; no committer pool member is stuck or spinning.

**Status:** PoC-must-pass.

### 3.3 Router death

**Trigger:** router BGWorker SIGKILL or panic.

**Detection:** postmaster sees BGWorker exit, restarts per standard policy.

**Recovery on router restart:**

The sweep iterates the staging queue. For each entry where `staging.valid.load(Acquire) == 2` (processing):

- Load `superbatch_id = staging.superbatch_id.load(Acquire)`. (Acquire pairs with the Release in the router's write path, ensuring the value is current — see §1.8 router step 5.j.)
- If `superbatch_id == 0`: the entry was claimed by the router but never assigned to a SuperBatch (router died mid-pack). CAS valid: 2 → 1 (pending). Re-routing happens on next tick.
- If `superbatch_id != 0`: was assigned to a SuperBatch. Look up CommitterQueueEntry by superbatch_id.
  - **Queue entry doesn't exist** (was never pushed before router died): CAS valid: 2 → 1 (pending). Re-route.
  - **Queue entry is `ready (1)`**: router pushed the queue entry but died before stamping all linked staging entries to valid=3. Per Step 14's CAS-2→0 fallback path, the committer's normal cleanup handles unstamped staging entries correctly when it claims this queue entry. Leave staging entry alone.
  - **Queue entry is `in_flight (2)`**: committer is actively processing OR a committer claimed it and died. Two sub-cases:
    - If the stored `(committer_bgw_slot, committer_bgw_generation)` identifies a live BGWorker (slot in use AND generation matches): leave staging entry alone; committer is processing.
    - If no live committer matches (slot empty OR generation advanced): orphan-recovery per §3.2 (lease takeover) handles the queue entry. The takeover committer's Step 14 cleanup will handle staging entry cleanup correctly. Leave staging entry alone here; let the takeover path own it.
  - **Queue entry is `completed (3)`**: per Step 14 cleanup ordering, this state is reached ONLY after ALL linked staging entries have been CAS'd to empty (0). Finding a staging entry at valid=2 (processing) linked to a `completed` queue entry is therefore unreachable — Step 14 transitions the queue entry to completed (3) only after all its staging entries are at 0. Defensive: log this state as an anomaly indicator (suggests a memory-ordering bug or external corruption), then CAS staging valid: 2 → 0 to reclaim the slot and free the arena. The §3.12 periodic audit serves as a secondary catch for any state truly stuck here.

**Test:** `test_v21_router_sigkill_pre_push`, `test_v21_router_sigkill_between_push_and_cas`, `test_v21_router_sigkill_post_cas`. For each: inject the kill at the named point, restart router, assert:
- No envelopes lost.
- No envelopes double-processed (dedup-lookup catches the rare double-route case).
- Staging slots eventually free.

**Test (memory ordering):** `test_v21_router_release_acquire_ordering`. Use a test-only barrier or sleep to delay the valid CAS after the superbatch_id store. Kill the router at the delayed point. Restart and run the sweep. Assert: every staging entry observed at valid=3 has a consistent superbatch_id (≠ 0); the sweep correctly identifies SuperBatch links without false-negatives that would mis-revert entries to pending.

**Test (router-died-mid-stamp):** `test_v21_router_dies_during_phase_6_stamp`. After router pushes CommitterQueueEntry (Phase 6 step (h) — valid=1 ready) but BEFORE the staging-entry stamping loop step (j) completes for all entries: SIGKILL router. Some staging entries are at valid=3 (stamped); others are at valid=2 (unstamped). A committer claims the CommitterQueueEntry, reads all staging entries (regardless of their valid state — the committer's claim path doesn't gate on valid==3), processes the SuperBatch normally. After commit, Step 14 cleanup encounters both valid=3 entries (normal CAS 3→0 path) and valid=2 entries (router-died-mid-stamp; CAS 3→0 fails, falls through to CAS 2→0). Assert: all staging slots reclaimed; all arena blocks freed exactly once; no slot leaks; no arena double-frees. Side effects in cost tables match the SuperBatch contents.

**Test (committer-dies-post-commit-pre-cleanup):** `test_v21_committer_dies_post_commit_pre_cleanup`. Committer processes SuperBatch, commits top-level transaction (cost rows are durable), then is SIGKILLed mid-Step-14 staging cleanup loop: some staging entries are at valid=0 (already cleaned), others at valid=3 (still routed); CommitterQueueEntry is still at in_flight (2) — the queue entry transitions to completed (3) only AFTER all staging entries are at 0. Assert: §3.2 orphan-recovery (lease takeover by another committer) reclaims the queue entry within `committer_lease_ms × 2`. The takeover committer's pg_xact lookup on the dead committer's committer_tx_id returns `committed`, so the takeover takes ownership of staging cleanup: CAS remaining staging 3 → 0, free arena, CAS queue entry 2 → 3 → 0. Cost rows are not duplicated (dedup-lookup short-circuits any partial re-execution). The §3.12 periodic audit serves as a secondary catch if §3.2 orphan-recovery is slow.

**Status:** PoC-must-pass.

### 3.4 Caller backend death after enqueue but before commit

**Trigger:** caller backend SIGKILL after `poc_v21_enqueue` succeeded but before caller's user-tx commits or aborts.

**Detection:** PG's normal backend-exit handling marks the user-tx as aborted in pg_xact.

**Recovery:**
- The staging entry written in lifecycle Step 6 is in shmem (not transactional); it remains in `pending` state.
- Whether a submission_status row exists at this point depends on the `status_insert_mode` GUC:
  - **`caller_intx`** (PoC default): the enqueue function INSERTs the status row with state='queued' INSIDE the caller's user-tx. If the caller commits, the row exists and the committer transitions it to terminal state. If the caller aborts, the row rolls back, AND the staging entry persists in shmem (write was outside the user-tx). When the committer pulls the staging entry, it detects an aborted user_tx_xid; the lazy fallback INSERT path applies (committer creates a submission_status row with state='failed', error_code='caller_tx_aborted'). Application tier observes the row at terminal state. Cheapest mode; correct under postmaster restart because committed callers have durable status rows that the recovery sweep finds.
  - **`committer_lazy`** (available only when `poc_v21.persistent_staging=on`): no INSERT at enqueue time; committer creates rows only at terminal-state determination. Has a fatal interaction with postmaster restart UNDER shmem-only staging (in-flight envelopes have no persistent status record AND no persistent envelope, so recovery sweep finds nothing and envelopes are silently lost). When persistent staging is on, the envelope itself is durable in `poc_v21_persistent_staging`, so the recovery sweep can find them via the persistent staging table even without status rows — and lazily create status rows during recovery. With persistent_staging=off, the extension refuses to set `status_insert_mode=committer_lazy` and errors at startup. Lowest enqueue overhead; requires persistent staging to be correct.

The PoC default `caller_intx` balances: cheapest enqueue path (no separate transaction), correct postmaster restart recovery (durable row exists for every committed caller), and acceptable observability (failed callers get a row created lazily by the committer on its first encounter). The bake-off measures `committer_lazy + persistent_staging` as the alternative for production deployments that want both durability of envelopes and cheapest enqueue (no status row in enqueue path).

**Failure visibility under caller abort.** When a caller's user-tx aborts (network failure, app crash, explicit ROLLBACK, statement_timeout, deadlock victimization), the path to operator visibility is:

1. The shmem staging entry persists with `user_tx_xid = T_caller`.
2. The router routes the entry to a committer on its next tick.
3. The committer's Step 4 checks `pg_xact_status(T_caller)`. If `aborted`: committer marks envelope failed.
4. The committer's lazy fallback runs `INSERT INTO poc_v21_submission_status (correlation_id, state='failed', error_code='caller_tx_aborted', ...) ON CONFLICT (correlation_id) DO NOTHING`.

Visibility latency: bounded by `min(caller_tx_timeout_ms, statement_timeout, network_keepalive_timeout)`. The PoC's default `caller_tx_timeout_ms = 30s` assumes deployments configure `statement_timeout >= 30s` (or unlimited). With these defaults, typical latency is one committer cycle (tens of ms); worst case is tens-of-seconds.

The ON CONFLICT DO NOTHING semantics make the lazy INSERT idempotent under re-routing: if an envelope is ejected and routed to a second committer that also observes the aborted user-tx, both committers may attempt the lazy INSERT. The PK conflict is silent and safe.

**Why `caller_subtx` is not a supported mode.** Earlier drafts of this spec defined a third mode, `caller_subtx`, intended to deliver status-row durability independent of the caller's user-tx outcome. Implementation discovered that PG sub-transactions are savepoints, not autonomous transactions: `BeginInternalSubTransaction` + `ReleaseCurrentSubTransaction` folds the sub-tx's writes into the parent's pending state. Row visibility still requires parent commit; row is still lost on parent abort. The mode delivered error isolation on the status INSERT (failure of the INSERT doesn't abort the caller's user-tx) but NOT abort survival.

Analysis of realistic failure modes for the status INSERT (caller bugs producing constraint violations, system OOM, table corruption) found the survival behavior insufficiently justified to retain a distinct mode. For callers requiring true durability under caller abort, use `committer_lazy` with `persistent_staging=on`. The persistent_staging row is WAL-logged within the caller's user-tx; on caller commit, it's durable; on caller abort, it's rolled back along with the caller's work, which is the correct semantics — an aborted submission was never durably submitted.

The historical caller_subtx mode is not a recognized value of `status_insert_mode`. Operators attempting to set it receive the standard PG `invalid value for parameter` error from the GUC catch-all validation; no dedicated rejection branch exists in the code.

**Test:** `test_v21_caller_sigkill_post_enqueue_intx` (default mode: assert 'queued' row exists in caller's user-tx; rolls back on abort; committer's lazy fallback creates 'failed' row on encountering aborted user_tx_xid). `test_v21_committer_lazy_blocked_without_persistent_staging` (asserts the extension refuses to start with `status_insert_mode = committer_lazy` AND `persistent_staging = off`; emits the expected hint). `test_v21_committer_lazy_with_persistent_staging` (asserts `committer_lazy` works when paired with `persistent_staging=on`; envelopes are recovered correctly after postmaster restart via the persistent staging table, status rows created lazily during recovery). `test_v21_caller_intx_abort_visibility_latency` (submit 100 envelopes, abort callers immediately without committing; measure latency from abort to status-row appearance via the lazy fallback; assert p99 within bounded latency expectations — a single committer cycle plus pg_xact propagation).

Postmaster-restart-related tests for caller-tx-mode interactions are in §3.6.

**Status:** PoC-must-pass. Both supported status_insert_modes must work in their valid configurations; `committer_lazy` without `persistent_staging` is gated.

### 3.5 Backpressure (staging queue full)

**Trigger:** staging queue is full when a caller attempts to enqueue.

**Detection:** caller checks ring fullness under the staging queue LWLock.

**Recovery:**
- Caller waits on backpressure CV up to `queue_full_timeout_ms`.
- Router drains entries, signals CV.
- Caller wakes, retries enqueue.
- If timeout elapses: caller's enqueue raises `ERRCODE_INSUFFICIENT_RESOURCES`. Caller's user-tx aborts.

**Test:** `test_v21_backpressure_blocks_then_unblocks` (fill queue; assert blocking; drain; assert unblock). `test_v21_backpressure_timeout` (fill queue; sleep past timeout; assert error).

**Status:** PoC-must-pass.

### 3.6 Postmaster restart

**Trigger:** postmaster restart, planned or otherwise.

**Detection:** shmem is fresh on `_PG_init`.

**Recovery:**

All shmem state is lost; staging and committer queues empty. Persistent state determines what is recovered. Startup recovery worker runs before queues accept new traffic.

**Recovery Phase 1: Replay durable envelopes (if persistent_staging=on).**

```sql
SELECT correlation_id, user_tx_xid, event_type, payload, sku_pool_keys,
       wip_pool_keys, business_date
FROM poc_v21_persistent_staging
WHERE state IN ('staged', 'in_shmem')
ORDER BY request_seq;
```

For each row:
- Check `pg_xact_status(user_tx_xid)`:
  - `committed`: caller's user-tx committed durably. Re-INSERT envelope into shmem staging queue with CAS valid: 0 → 1 (pending). Set persistent_staging.state to `in_shmem`. The router picks it up on next tick and processing resumes.
  - `aborted`: caller rolled back; persistent_staging row is orphaned. DELETE the row. Under `caller_intx` mode, no submission_status row exists (rolled back with caller's user-tx); no action needed. Under `committer_lazy + persistent_staging`, INSERT a `state='failed', error_code='caller_tx_aborted'` row directly via `ON CONFLICT (correlation_id) DO NOTHING`.
  - `in_progress`: defensive — shouldn't happen after postmaster restart (all backends killed → all txs aborted). Treat as aborted.
- BEFORE the above: check if cost rows already exist for this correlation_id (`SELECT 1 FROM poc_v21_cost_layers WHERE correlation_id = $1 LIMIT 1` etc). If yes: the committer committed before the crash. Set persistent_staging.state = 'completed'; set submission_status row to 'committed'. No re-enqueue.

**Recovery Phase 2: Sweep submission_status for non-durable envelopes.**

```sql
SELECT correlation_id FROM poc_v21_submission_status
WHERE state IN ('queued', 'processing')
  AND correlation_id NOT IN (SELECT correlation_id FROM poc_v21_persistent_staging);
```

For each row (i.e., non-durable envelopes that didn't make it through):
- Query cost rows by correlation_id.
  - If cost rows exist: committer committed before crash. UPDATE state='committed' (or 'replayed' if appropriate).
  - If no cost rows: the envelope was lost in shmem. UPDATE state='failed' with error_code='postmaster_restart_loss'.

**Recovery Phase 3: Sequence initialization.**

Initialize per-pool `next_born_seq` and `next_consumed_seq` counters by reading MAX from cost tables. This is done lazily on first committer access per pool, not eagerly at startup (per §7 hydration).

**Recovery Phase 4: Queues accept new traffic.**

The router and committers resume normal duty.

**Test:** `test_v21_postmaster_restart_caller_intx`. Mid-load with `durable_queue=false` in caller_intx mode: SIGKILL postmaster, restart. Assert: committed-caller rows intact, in-flight envelopes (where the caller committed but committer hadn't yet) marked 'failed' with error_code='postmaster_restart_loss'; uncommitted callers have no submission_status rows at all. System resumes new traffic normally.

**Test:** `test_v21_postmaster_restart_durable_queue`. Mid-load with `durable_queue=true` and `persistent_staging=on`: SIGKILL postmaster, restart. Assert: NO envelopes from committed callers are lost. All replayed via the persistent staging path. System resumes new traffic. Compare in-flight loss rate against the caller_intx test to validate the durability claim.

**Test:** `test_v21_postmaster_restart_mixed`. Mid-load with a mix of `durable_queue=true` and `durable_queue=false` submissions: SIGKILL postmaster, restart. Assert: durable submissions are all recovered; non-durable in-flight submissions are marked failed; per-correlation-id classification is correct.

**Status:** PoC-must-pass.

### 3.7 Per-envelope business-logic failure

**Trigger:** `plan_apply` returns error for one envelope (e.g., InsufficientInventory in FIFO consumption).

**Detection:** result.per_event[].error.is_some() OR result.envelope_error.is_some().

**Recovery:**
- Per-envelope failure isolation: failed envelope's rows excluded from Step 5 UNNEST arrays.
- Other envelopes in SuperBatch commit normally.
- Failed envelope's submission_status updated to 'failed' with appropriate error_code.

**Test:** `test_v21_per_envelope_failure_isolation`. Submit batch of 10 envelopes where envelope 3 has InsufficientInventory; assert 9 commit normally, envelope 3 is failed.

**Status:** PoC-must-pass.

### 3.8 Lease takeover false positive

**Trigger:** committer is doing legitimate slow work (e.g., deep snapshot hydration with continuation fetches). Lease expires while still processing.

**Detection:** another committer scanning the queue sees stale lease.

**Recovery:**
- Before stealing, contending committer verifies that the stored `(committer_bgw_slot, committer_bgw_generation)` still identifies a live BGWorker. If it does (slot in use, generation matches), the holder is alive: back off. Re-read committer_acquired_at_ns (the holder may have updated it), sleep `committer_lease_ms`, retry check.
- A legitimately-slow committer holding the role for longer than lease blocks the shard until it finishes; mitigate via tighter batch_size_max so per-batch time stays well below lease.

**Test:** `test_v21_slow_committer_not_stolen` (inject 2× lease sleep into plan_apply; assert no takeover). `test_v21_dead_committer_stolen` (kill committer; assert takeover within 2× lease).

**Status:** PoC-must-pass.

### 3.9 Router head-of-line starvation (defensive — should not arise in single-router PoC)

**Trigger:** an envelope at the staging queue head is repeatedly skipped over multiple router ticks. Under affinity grouping in single-router PoC, this should NEVER happen — the oldest-first sort by min(request_seq) on connected components guarantees the group containing the head envelope is dispatched first in each tick. This section is retained for two reasons: (1) defense against future multi-router design where two routers racing on CAS could repeatedly skip the same entry, and (2) edge cases under shutdown/restart sequences.

**Detection:** entry's starvation_tick_count exceeds router_starvation_threshold_ticks.

**Recovery:**
- Force the candidate as a size-1 SuperBatch on next router tick (bypassing affinity grouping for this one envelope).
- Slow, but progress is made.

**Test:** `test_v21_router_head_of_line_progress`. Workload: 1000 envelopes targeting the same single SKU. Affinity grouping puts all 1000 into one connected component; under batch_size_max=50, this splits into 20 SuperBatches that serialize via FOR UPDATE. Assert: all envelopes processed in submission order; no envelope's staging time exceeds (starvation_threshold_ticks × tick_interval × 2) significantly. The test validates that the affinity-group ordering preserves request_seq order.

**Status:** PoC-must-pass.

### 3.10 Spillover arena exhaustion

**Trigger:** spillover arena fills with payloads, pool_keys arrays, and staging-entry-offset arrays.

**Detection:** arena allocator returns out_of_arena to enqueue path or router.

**Recovery:**
- Enqueue path: treat as queue-full (same backpressure CV wait).
- Router path: can't assemble new SuperBatches until arena drains. Naturally backpressures the staging queue.

**Slot lifecycle coupling:** when a SuperBatch completes, the committer must free its spillover blocks (staging-entry-offsets array, pool_keys arrays) before freeing committer queue slot. Likewise, freed staging entries free their payload blocks. Forgotten coupling = arena leak.

**Test:** `test_v21_spillover_exhaustion`. Undersized arena; sustained large-payload workload; assert clean backpressure propagation.

**Status:** PoC-must-pass.

### 3.11 Caller user-tx in_progress eject loop exhausted

**Trigger:** caller's user-tx remains `in_progress` for an unreasonably long time. Each committer that pulls a SuperBatch containing this envelope ejects it back to pending; the router re-routes; the cycle repeats.

**Detection:** committer's Step 4 check observes `now_micros - enqueued_at_micros > caller_tx_timeout_ms × 1000` (default 30 seconds; the primary wall-clock bound) OR `staging.eject_count > max_eject_count` (default 10000; defensive safety bound against pathological cycling).

**Recovery:**
- Mark envelope as 'failed' with error_code='caller_tx_eject_exhausted' or 'caller_tx_timeout' depending on which threshold fired.
- Exclude from current batch. Free staging slot. Continue with rest of batch.
- Caller will eventually commit or abort their user-tx; if committed, the cost rows for this envelope never happened (the work was dropped). The caller's contract is "you must commit promptly after enqueue."
- Log loudly: legitimate caller patterns should never hit this. If the bake-off observes the threshold firing under realistic workload, the threshold is too tight or callers are misbehaving.

**Why this design instead of committer-sleeps-waiting:**

The committer pool is small (typically 4-16 BGWorkers). If a committer sleeps waiting on a caller's commit, that's one less committer available to process other SuperBatches. Under high concurrency where every caller is mid-commit at any moment, all committers could be sleeping simultaneously while staging queue and committer queue accumulate. The pool stalls under exactly the load v2.1 is designed to absorb.

Ejection avoids this entirely. A committer never sleeps for any caller-related reason. When ejection happens, the envelope returns to the router's pending pool; on the router's next tick, the envelope may have transitioned to committed/aborted (committer processes it normally) or may still be in_progress (cycle repeats, eject_count increments). The router and committer pools stay productive; only the specific envelope waits.

The cycle overhead per envelope is one router tick + one committer pg_xact check + one staging-slot CAS. Negligible compared to a sleeping committer.

**Test:** `test_v21_caller_eject_loop_terminates`. Inject a caller that holds its user-tx open for `caller_tx_timeout_ms + 1000ms`; assert: envelope is ejected and re-routed repeatedly; eject_count grows; eventually marked 'failed' with appropriate error_code; committer pool throughput is NOT degraded during the eject loop (other envelopes continue processing).

`test_v21_eject_does_not_stall_committer_pool`. Inject 100 concurrent callers each holding their user-tx for several seconds; submit 1000 envelopes; assert committer pool throughput remains within 10% of nominal (committers cycle through ejecting these envelopes and processing other work).

**Status:** PoC-must-pass. This is critical for the committer-non-blocking invariant: a committer must never sleep waiting on a caller's user-tx transition.

### 3.12 Slot leak audit

**Trigger:** slots in staging or committer queue stuck in non-terminal states due to bugs.

**Detection:** periodic audit (every 60s). The reachable stuck-slot states, given the Step 14 cleanup ordering (clear staging entries 3→0 → CAS queue entry 2→3), are:

- **Staging entries in `processing (2)` for >60s with dead backend_pid.** The enqueuing caller died after CAS-claiming the staging slot but before publishing the envelope. CAS valid: 2 → 0; free arena.
- **Staging entries in `routed (3)` for >5min with no live CommitterQueueEntry referencing them.** Recovery sweep missed them, or the queue entry was reaped without cleaning staging. Mark submission_status failed (error_code='orphaned_staging_audit'); CAS valid: 3 → 0; free staging arena.
- **CommitterQueueEntry in `in_flight (2)` for >5min where the stored (committer_bgw_slot, committer_bgw_generation) no longer identifies a live BGWorker.** Two sub-cases distinguished by committer_tx_id pg_xact lookup:
  - committer_tx_id is `aborted` or no tx_id was assigned (committer died before tx commit): rollback path. CAS queue entry to `ready (1)` for re-claim by another committer; staging entries stay at routed=3 to be re-processed.
  - committer_tx_id is `committed` (committer died after tx commit, before Step 14 staging cleanup): post-commit-pre-cleanup case. Cost rows are durable. Audit takes ownership of cleanup: for each linked staging entry, CAS valid: 3 → 0 and free its arena; free the CommitterQueueEntry's OWN arena blocks (staging_entry_offsets array, sorted pool_keys arrays — queue-entry-owned, not staging-entry-owned); CAS queue entry: 2 → 3 (completed), then 3 → 0 (reusable) on next audit pass or inline.
- **CommitterQueueEntry in `completed (3)` for >5min:** routine cleanup case. CAS 3 → 0 to free the slot. (Normally the next committer's tick claim path advances this; the audit is a safety net.)

(Note: the previously-documented audit case "staging entries in `routed (3)` linked to a CommitterQueueEntry in state `completed (3)` with dead committer identity" is unreachable given Step 14 cleanup order. The committer transitions the queue entry from in_flight (2) to completed (3) ONLY AFTER it has CAS'd all linked staging entries to empty (0). A dead committer leaves the queue entry at in_flight (2), not completed (3). The detection logic above reflects this correctly.)

**Test:** `test_v21_slot_leak_audit_reclaims`. Manufactured leak (kill backend mid-allocation); assert audit reclaims within 70s.

**Status:** PoC-should-pass (safety net).

### 3.13 Shmem corruption (best-effort)

**Trigger:** bug or external corruption invalidates an atomic or ring buffer pointer.

**Detection:** validators in router and committers detect impossible states (head > tail, slot state value out of range).

**Recovery:** log + PANIC. Postmaster restart per §3.6 picks up.

**Test:** `test_v21_shmem_corruption_panic`. Test-only function writes garbage to shmem; assert detection + PANIC + clean restart.

**Status:** Best-effort.

---

## 4. Validation Criteria

Pass/fail bars. PoC is a pass only if all must-pass criteria pass. Conditional pass allowed if 1-2 should-pass criteria fail with documented mitigations.

### 4.1 Correctness criteria (must-pass)

**C1.** All §3 PoC-must-pass failure modes recover correctly. Each named test passes.

**C2. Invariants hold under property-based testing.** proptest harness generates random sequences of (apply, abort, kill, restart) operations across multiple backends and methods. After every step:

- **I1**: For every depletion, the referenced layer's effective_qty ≥ depletion.qty.
- **I4/I5**: consumed_seq monotone within (layer_id, consumed_at); born_seq monotone within (sku_id, location_id, born_at).
- **I-row-unique**: UNIQUE constraints never trigger in correctness path (only as bug-finding safety net). If proptest triggers a UNIQUE violation, that's a test failure.
- **I-row-attribution**: every cost row has valid correlation_id, user_tx_xid, committer_tx_id, superbatch_id.
- **I-replay-idempotent**: retry of an envelope with same correlation_id and same (issue_id, method) tuples produces row-identical result; UNIQUE never triggers; submission_status terminal state is 'replayed'.
- **I-eventual-resolution**: every envelope reaches terminal state in submission_status within `MAX_RESOLUTION_BOUND = 10 × committer_lease_ms` of any failure event affecting it. No envelope stuck in 'queued' or 'processing' indefinitely.
- **I-caller-tx-honored**: if a caller's user-tx aborts (verified via pg_xact), no cost rows attributable to that user_tx_xid exist in cost tables.
- **I-pool-snapshot-consistency**: within a SuperBatch, events that touch the same pool are applied in chronological order (per the §1.8 Step 4 sort) against a shared per-pool in-memory snapshot. After event N, the snapshot reflects the cumulative effect of events 1..N. Event N+1 (if it touches the same pool) sees the post-event-N state. Verified by injecting two envelopes that both touch SKU X (e.g., receipt then consume) into one SuperBatch; assert the consume sees the receipt's layer.
- **I-affinity-grouping**: at the moment of SuperBatch assembly, for every pair of envelopes (A, B) that share at least one pool_key AND both are pending in the same router window, A and B are in the SAME SuperBatch (or in chunks of the same connected component if the component exceeds batch_size_max). Verified by `test_v21_affinity_groups_overlapping_envelopes`: inject [Env_A on SKU 5, Env_B on SKU 6, Env_C on SKU 5] with batch_size_max=2, assert Env_A and Env_C land in the same SuperBatch, Env_B in a different one.
- **I-router-progress**: no envelope sits in `pending` state longer than `batch_window_us × 3` under any workload (with single-router PoC's oldest-first group dispatch, the head's group is always claimed within one tick).
- **I-committer-non-blocking**: no committer is ever observed sleeping waiting on a caller's pg_xact transition. Verified by injecting a controlled-duration slow caller and asserting that committer-pool throughput on non-overlapping envelopes is undegraded by the slow caller's existence.
- **I-eject-bounded**: any envelope subjected to eject cycles reaches terminal state within `max_eject_count + 1` eject events OR `caller_tx_timeout_ms` wall time, whichever fires first.
- **I-causal-snapshot-observability**: when committer A's top-level transaction commits while committer B's transaction is blocked on FOR UPDATE for the same pool, committer B's subsequent snapshot hydration SELECT observes committer A's committed rows. Verified via `test_v21_causal_chain_concurrent` and via a dedicated proptest scenario that injects ordering races.
- **I-upsert-array-unique**: every bulk UPSERT array passed to a Step 5 INSERT ... ON CONFLICT DO UPDATE statement contains exactly one entry per conflict-target key. Specifically, the avg_pool_state UPSERT input arrays contain one row per (sku_id, location_id); the persistent_staging completed-transition UPDATE matches one row per correlation_id; the Step 12 status UPSERTs match one row per correlation_id. Verified by `test_v21_avg_pool_state_dedup_within_superbatch`: inject a SuperBatch with multiple envelopes targeting the same AVG SKU pool (e.g., 3 receipts into SKU 5); assert the committer's UPSERT call carries ONE entry for SKU 5 with the final cumulative state, not three entries. A naive implementation that appends per-event would hit PG's "ON CONFLICT DO UPDATE cannot affect row a second time" abort.

**C3. Determinism.** Fixed event stream, fixed seed, no failure injection: rows written to cost tables are byte-identical across runs. Verified via test harness replay.

**C4. Idempotency under retry.** Caller retries enqueue with same correlation_id and same (issue_id, method) tuples after the original committer committed. Assert: dedup-lookup hits; no duplicate rows; submission_status updated to 'replayed' for the retry's correlation_id (different from original).

(Note: the retry uses a DIFFERENT correlation_id because correlation_id is meant to be unique per submission attempt. The replay detection is on (issue_id, method), not on correlation_id. Re-using correlation_id is a separate test case — should be rejected by submission_status UNIQUE PK.)

**C5. Causal-chain correctness.** A workload of causally-dependent envelopes (PoReceipt → WoComplete consuming the received component → SoShipment of the WoComplete's output) submitted at low inventory levels produces correct results. Specifically:

- **C5.1 — Success path.** Margin ≥ 0 triplets all commit. No spurious InsufficientInventory failures. Verified by `test_v21_causal_chain_success`.
- **C5.2 — Concurrent ordering.** Triplets submitted simultaneously from multiple backends produce identical final state to sequential submission. The lex-lock + snapshot-hydration pattern correctly observes upstream-committed work via READ COMMITTED snapshot refresh (see §1.8 Step 2 isolation note). Verified by `test_v21_causal_chain_concurrent`.
- **C5.3 — Mid-chain failure cascade.** When an envelope in the middle of a chain fails (E2's SuperBatch tx aborted), the upstream envelope's state is preserved (E1 commit unaffected) and the downstream envelope fails with InsufficientInventory citing the correct SKU (E3 fails on Assembly-Y, not on Component-X). Verified by `test_v21_causal_chain_failure_cascade`.
- **C5.4 — True shortfall attribution.** When the workload is genuinely under-resourced (margin = -1), failures are attributed to the correct envelope and SKU. WoComplete fails with InsufficientInventory citing Component-X; SoShipment fails with InsufficientInventory citing Assembly-Y. The error_detail JSON contains the offending SKU and the qty shortfall. Verified by `test_v21_causal_chain_true_shortfall`.

Test specifications:

**`test_v21_causal_chain_success`** (C5.1): Setup empty inventory; submit 100 triplets sequentially from 1 backend with margin=5 (each PoReceipt brings 15 units; WO consumes 10, leaves 5). Assert all 300 envelopes commit, final Component-X inventory = 500 units, Assembly-Y = 0, no InsufficientInventory errors.

**`test_v21_causal_chain_concurrent`** (C5.2): Setup empty inventory; submit 100 triplets concurrently from 16 backends (each backend's chain on a backend-assigned distinct SKU pair to avoid cross-backend contention on the same Component-X). Margin=5. Assert identical final state to sequential test. Critical: verify the lex-lock pattern observed E1's rows during E2's snapshot hydration. If isolation level is wrong, this test fails with InsufficientInventory on E2.

**`test_v21_causal_chain_failure_cascade`** (C5.3): Submit triplet E1, E2, E3 from one backend. Inject tx failure (constraint violation simulation) in E2's Step 5. Assert E1 committed, E2 failed (no cost rows), E3 fails with InsufficientInventory citing Assembly-Y. error_detail JSON contains shortfall information.

**`test_v21_causal_chain_true_shortfall`** (C5.4): Submit triplet with margin=-1 (PoReceipt=8, WO needs 10, SO needs 1 of Assembly-Y). Assert E1 committed, E2 fails with InsufficientInventory citing Component-X (shortfall=2), E3 fails citing Assembly-Y (shortfall=1). Final Component-X = 8 (PoReceipt's qty, untouched by failed WO); Assembly-Y = 0.

**Notes on correlation_id, retries, and visibility latency (descriptive, not contract-mandating):**

*Correlation_id idempotency.* Dedup-lookup operates on `(correlation_id, issue_id)` where `issue_id` is derived from event content. Callers using correlation_id as an idempotency token should ensure same-correlation_id implies same-content; submitting different content under the same correlation_id may produce multiple cost-row sets because dedup-lookup will not match. Callers can guarantee content stability by deriving correlation_id from a canonical hash of the envelope payload (e.g., UUIDv5 over a canonical serialization). This is a caller-side practice, not enforced by the extension.

*Unique violation on retry.* `poc_v21_enqueue` raises `unique_violation` when the correlation_id is already present in `poc_v21_submission_status`. Caller retry behavior on this signal is application-specific: callers using correlation_id as an idempotency token typically treat it as "envelope already submitted; poll status for outcome"; callers using fresh correlation_ids per attempt should not encounter the situation. The extension does not prescribe a retry policy.

*Failure visibility latency.* Failed envelopes appear in `poc_v21_submission_status` with bounded latency, but not instantaneously. The committer's lazy fallback fires on its next encounter with the envelope after the caller's user-tx transitions to aborted in `pg_xact`. For polling clients, expect tens-of-milliseconds latency under typical load; up to `caller_tx_timeout_ms` (PoC default 30s) under pathological caller stalls. See §3.4 "Failure visibility under caller abort" for the full mechanism.

### 4.2 Performance criteria (must-pass)

**P1. Low-overlap workload scales with backend count.** Workload: fan_out (g=5000 SKUs, K=5 wo_complete events, 32 backends each pinned to disjoint SKU subsets — so envelopes from different backends touch independent pools). Throughput at N=32 ≥ 20× throughput at N=1. (The single-threaded router caps strict linear scaling; 20× represents the realistic ceiling under affinity grouping.)

**P2. High-overlap workload colocates correctly.** Workload: fan_contested (g=50 SKUs, K=5, 16 backends competing for overlapping component sets). Within each router window, many envelopes touch the same SKUs; v2.1 packs them into the same SuperBatch and the committer handles overlap via shared snapshot. Expected: throughput at N=16 within 1.5× of N=1 for the overlapping pool's portion (one committer serializes the contended work via plan_apply's in-memory mutation, no inter-committer FOR UPDATE waits). Critical: no consistency violations, no SSI errors, no double-cost rows.

**P3. Hot-pool throughput measured.** Workload: hot_pool (one SKU consumed by 80% of events, others varied). Under v2.1's design, hot-pool events are colocated into one SuperBatch and processed by one committer; other committers run parallel SuperBatches on the unhot 20%. Expected: hot-pool throughput per committer matches single-committer ceiling (~1-6K SuperBatches/sec from B2); aggregate throughput scales with non-hot work in parallel SuperBatches.

**P4. Bulk-UNNEST efficiency.** Workload: large_batch (batch_size_max=200 envelopes, K=15 components each = 3200 events per SuperBatch). Throughput should be substantially higher than batch_size_max=1 case (target: 50× — direct measure of UNNEST amortization).

**P5. Multi-target UNNEST not a bottleneck.** With 6 UNNEST INSERTs/UPSERTs per SuperBatch (vs theoretical 1), measure overhead. Throughput should be within 25% of the "merged into 1 INSERT" baseline (test: do a calibration run with all rows merged into one synthetic flat table to compare).

**P6. p99 latency at moderate load.** With 16 backends and low-overlap workload at 50% of measured peak: p99 enqueue-to-committed latency < 100ms. (Reflects the staging-to-router-to-committer hop budget; tighten in production tuning after baseline.)

Publish full latency curve (p50, p99, p99.9 vs load%) for each shape.

**P7. Mixed-method workload.** Workload with 33% FIFO, 33% AVG, 33% STD, mixed pools. Throughput within 30% of best single-method workload on same shape.

### 4.3 Router-specific criteria (must-pass for v2.1)

**R1. Router packing under low-overlap workloads.** When the workload generates pool-disjoint envelopes (fan_out_simple or fan_out_wo shapes), each connected component contains 1 envelope. The router builds many small SuperBatches per tick, bounded by free CommitterQueueEntry slots. Expected: per-tick SuperBatch count matches the count of distinct pool-key groups in the window, capped by committer queue depth.

**R2. Router packing under high-overlap workloads.** When the workload generates envelopes that overlap heavily (fan_contested_wo or fan_in_wo shapes), envelopes that share pool_keys are grouped together. Average envelopes-per-SuperBatch grows toward batch_size_max as overlap density increases. Expected: workload where N envelopes all touch the same SKU produces 1 connected component, packed into ⌈N / batch_size_max⌉ SuperBatches. Single-SKU workloads should produce SuperBatches at exactly batch_size_max except for the tail.

**R3. Affinity correctness.** For any envelope pair (A, B) sharing a pool_key and both pending in the same router window: A and B must be in the same SuperBatch OR in chunks of the same connected component split by batch_size_max. Verified by `test_v21_affinity_groups_overlapping_envelopes`: dedicated test injecting interleaved overlapping envelopes ([SKU 5, SKU 6, SKU 5] with batch_size_max=2) and asserting the SKU-5 envelopes land in the same SuperBatch.

**R4. Router throughput.** Router can pack at least 3× the rate at which the committer pool can drain. (Validates router not a bottleneck under healthy load. Affinity-grouping cost is well within the analytical ceiling per §2.1 B3.)

**R5. Head-of-line progress.** No envelope sits in pending state longer than 2 × batch_window_us under any non-saturated workload. (Validates the oldest-first group dispatch.)

### 4.4 Operational criteria (should-pass)

**O1. 7-day soak test at 50% of peak.** Mixed workload, continuous run. Pass criteria:
- No slot leaks (audit reports 0 reclaims).
- No memory leaks (shmem high-water mark stable ±5%).
- No throughput degradation > 10%.
- No unexplained errors.

**O2. Recovery time SLAs.**
- Committer death takeover: < 2 × committer_lease_ms p99.
- Router death + restart: < 5s p99 (postmaster bgworker restart is bounded).
- Backpressure recovery on drain: < 10ms p99.
- Postmaster restart with 10K in-flight envelopes: < 60s.

**O3. Observability metrics.**
- Per-queue depth via `poc_v21_staging_stats()`, `poc_v21_committer_stats()`.
- Router affinity-grouping stats (component-size distribution, average envelopes-per-SuperBatch, cross-SuperBatch FOR UPDATE wait count) via `poc_v21_router_stats()`.
- Per-method dispatch counts via `poc_v21_method_stats()`.
- Recovery events counted (lease takeovers, abandoned slots, replays).

### 4.5 Hardening-phase criteria (deferred from must-pass)

**H1.** Shmem corruption detection (§3.13). Best-effort only.

**H2.** Spillover arena exhaustion under pathological load (§3.10). Verified under contrived test.

**H3.** Long-running replay chains. A workload that retries every envelope ten times. Performance acceptable.

---

## 5. Bake-off Methodology

### 5.1 Hardware (fixed for PoC) and pre-bake-off calibration

Document concrete specs in `V21_BENCHMARK_RESULTS.md`. The PoC assumes single-machine PG primary with NVMe storage, ≥16 CPU cores, sufficient RAM to keep working set in buffer pool.

**Pre-bake-off calibration (mandatory before §5.2 workload runs):**

1. **Fsync latency profiling.** Run pg_test_fsync (or equivalent) for 60s; record p50, p99, p99.9 fsync latency. Use this to set `committer_lease_ms`:
   - `committer_lease_ms = max(100, 10 × fsync_p99_ms)`
   - On NVMe with p99 ~200μs, this stays at 100ms (the default). On slower storage, the lease grows to avoid false orphan recovery during legitimately-slow commits. Document the chosen value.

2. **Cold-start warmup measurement.** Before each bake-off cell, the staging queue is empty and `pool_locks` / `wip_pool_locks` tables are empty for the workload's SKU set. The first envelopes pay lazy-creation cost. Measure throughput during the first 5 seconds separately from the steady-state measurement; report both.
   - Steady-state throughput: events/sec averaged over the 60s run AFTER the first 5s.
   - Cold-start throughput: events/sec averaged over the first 5s only.
   - The delta informs production deployment: if cold-start throughput is < 50% of steady-state, lazy creation is significant and eager pre-creation should be considered (§14 A3 in design-v2.1).

### 5.2 Workload shapes

Each shape parameterized by:
- `g`: number of distinct SKUs (controls fan-out)
- `K`: components per wo_complete event (1, 5, 15, 50)
- `N`: concurrent backend count
- Method mix: FIFO-only, AVG-only, STD-only, or mixed-33-33-33
- Event mix: 100% inv_adjust (simple single-pool) vs 100% wo_complete (multi-pool) vs 50/50

**Shape S1: fan_out_simple.** g=5000, event=inv_adjust, K=1, varies N. N backends pinned to disjoint SKU subsets. Each backend issues random inv_adjust events. Tests the degenerate single-pool case; baseline for amortization measurements.

**Shape S2: fan_out_wo.** g=5000, event=wo_complete, K=5, varies N. Each event consumes 5 random components from the backend's SKU subset; produces 1 output. Tests primary v2.1 multi-pool workload with low overlap (each backend has its own SKU subset).

**Note on K-value semantics:** for a `wo_complete` event with parameter K, the actual pool_keys array carries K+2 entries: K component SKU pools (raw materials being consumed) + 1 WIP pool (`(work_order_id, operation_id)`) + 1 output SKU pool (finished/intermediate good being produced). The lock-acquisition fan-out per envelope is therefore K+2, not K. The committer's FOR UPDATE operates on the SuperBatch's deduplicated union across all envelopes' pool_keys.

**Shape S3: fan_contested_wo.** g=50, event=wo_complete, K=5, varies N. Backends share SKU pool; many envelopes touch overlapping pools. Tests v2.1's affinity-grouping: overlapping envelopes are routed into the same SuperBatch by the router's union-find, and one committer handles them via shared in-memory snapshot mutation. Expected: minimal inter-committer FOR UPDATE contention; throughput limited by single-committer pipeline rate on the contended portion. CRITICAL test: validates that affinity grouping fires correctly for envelopes arriving from concurrent backends (non-consecutive arrival in staging queue but routed together by grouping).

**Shape S4: fan_in_wo.** g=1 (one SKU). All events touch the same SKU. Extreme overlap test: every envelope shares the SKU. The router groups all pending envelopes into ONE connected component, then splits into ⌈window / batch_size_max⌉ SuperBatches. The split SuperBatches serialize via FOR UPDATE on pool_locks; one committer processes them sequentially. Throughput cap: single-committer rate × batch_size_max (per-batch amortization preserved within each split chunk; cross-chunk FOR UPDATE is the genuine fallback for oversized components).

**Shape S5: hot_pool.** g=100 SKUs, with one SKU receiving 80% of traffic, others uniformly. Tests mixed-overlap workload: hot-pool envelopes form one connected component (all sharing the hot SKU) and are grouped together by affinity; non-hot envelopes form smaller groups based on their own pool overlaps. Expected: hot-pool component processed by one committer at single-committer rate; non-hot components run as independent SuperBatches on other committers in parallel.

**Shape S6: large_wo.** g=5000, event=wo_complete, K=15. Tests larger BOMs. Lock acquisition fan-out per SuperBatch grows.

**Shape S7: very_large_wo.** g=5000, event=wo_complete, K=50. Tests deep BOMs. Stress test for lex-lock chain length.

**Shape S8: mixed_event_mixed_method.** g=1000, 50% inv_adjust + 50% wo_complete K=5, methods=mixed. Tests realistic blended workload.

**Shape S9: causal_chain.** g=10 raw component SKUs + 5 assembly SKUs = 15 total SKUs. Workload generator produces triplets:
- `PoReceipt` of qty `R` units of one random component SKU.
- `WoComplete` consuming qty `R - margin` units of that same component SKU, producing 1 unit of one random assembly SKU. The `margin` parameter controls how much excess inventory is left after the WO (margin=0 means WO consumes everything PoReceipt brought in; margin=R/2 leaves half).
- `SoShipment` of 1 unit of that assembly SKU.

Each triplet is submitted from one backend with no inter-envelope delay. Triplets across backends are pool-disjoint by SKU assignment per backend.

Bake-off parameters:
- Margin sweep: margin ∈ {-1, 0, 1, 5} where margin=-1 represents the WO consuming MORE than the PoReceipt provides (must fail with InsufficientInventory; tests failure attribution per C5.4).
- N backends: 1, 4, 16. Tests both single-backend ordering and multi-backend concurrent chains.

Workload-level invariants asserted at end of run:
- **For margin ≥ 0**: every triplet committed successfully. No InsufficientInventory failures. Final inventory state matches expected: Component-X = `margin × N_triplets`, Assembly-Y = 0 (all shipped).
- **For margin = -1**: every triplet's WoComplete failed; every triplet's SoShipment failed (cascade). PoReceipts succeeded.

S9 is the workload that exercises C5 causal-chain correctness directly. It validates the §1.8 Step 2 READ COMMITTED isolation requirement: any implementation that runs the committer's transaction at a stricter isolation level fails this workload with spurious InsufficientInventory errors on E2.

### 5.3 Run methodology

For each (shape, N, method_mix, GUC) cell:
1. Setup (~30s):
   - Seed cost tables with 10 layers per SKU, varied unit_cost.
   - For every SKU assigned to the AVG method: INSERT a corresponding row into `poc_v21_avg_pool_state` with the correct seed values computed from the seeded layers: `avg_unit_cost = SUM(qty * unit_cost) / SUM(qty)` and `avg_total_qty = SUM(qty)` over that SKU's layers. The first committer to encounter an AVG pool MUST see a valid row in this table; an absent row indicates a setup bug, not a runtime case, and the committer should error rather than treat it as "average is zero." (At runtime in production, an AVG pool is created by its first receipt event, which is responsible for inserting the initial state row; PoC setup mimics this by pre-seeding.)
   - For every SKU assigned to the STD method: ensure `poc_v21_standard_costs` has at least one row with `effective_from <= NOW()`. (Standard costs are configuration; absence is a setup bug.)
   - Reset shmem. Warm cache.
2. 5 runs of 60s each. Backends taskset-pinned. Between runs, 30s rest.
3. Per-run measurements:
   - Throughput: events/sec (committed event count / wall time).
   - Latency histogram: enqueue-to-committed via correlation_id timing.
   - `pg_locks_sampler` at 100ms intervals (lock waits).
   - Per-shard queue depth samples at 1s.
   - Router affinity stats: envelopes/SuperBatch histogram, connected-component-size distribution, starvation forced count, cross-SuperBatch FOR UPDATE wait count.
   - Committer takeover count, abandonment count, dedup-replay count.
4. Statistical analysis: median + IQR across 5 runs. IQR > 10% of median flags noise.

### 5.4 Backend count sweep

Per shape: N ∈ {1, 2, 4, 8, 16, 32, 64, 128}. (Capped at 128; beyond this the router is likely the binding constraint regardless of workload because the router is single-threaded.)

### 5.5 GUC sweep (limited)

On shape S2 and S3:
- `batch_size_max`: 1, 10, 50, 200
- `batch_window_us`: 100, 500, 2000
- `router_window_size`: 100, 1000, 10000
- `synchronous_commit`: on, off (off voids durability — flagged prominently in results)

On shape S2 specifically:
- `skip_wip_locks`: false (default), true (test-only flag bypassing wip_pool_locks acquisition). Validates the §1.5 / §7 Q-G claim that WIP locks are uncontended; if measured throughput delta is < 5%, production deployment can drop WIP locks.

On shape S2 + S3, mode comparison:
- `status_insert_mode`: caller_intx (default), committer_lazy (only with persistent_staging=on). Measures committer_lazy + persistent_staging cost vs caller_intx baseline.

On shape S2 + S8, durability comparison:
- `poc_v21.persistent_staging`: off (default), on. With persistent_staging=on, sweep `durable_queue` request rate ∈ {0%, 25%, 50%, 75%, 100%} of submissions. Measures:
  - Per-enqueue overhead of persistent_staging INSERT (durable_queue=true).
  - Committer-side overhead of the `staged → completed` transition (bundled into Step 5 bulk UNNEST UPDATE; runs only when durable envelopes are present).
  - Total throughput delta vs the default shmem-only path.
  - Postmaster-restart recovery time as a function of persistent_staging row count.
  Expected: 5-30% per-enqueue overhead at 100% durable_queue rate; minimal impact on committer pipeline; recovery time scales linearly with in-flight persistent_staging row count.

On shape S7:
- `snapshot_layer_limit_per_pool`: 100, 1000, 10000

### 5.6 Bottleneck classification per cell

Each cell labeled with binding bottleneck from {B1 WAL, B2 SPI, B3 router, B5 lock-contention, CPU-other}.

Metrics:
- **B1 WAL**: pg_stat_wal.wal_bytes and wal_fsync_time deltas; if observed fsync rate ≥ 80% of hardware's measured fsync ceiling → B1.
- **B2 SPI**: cumulative SPI time / wall time per committer; if > 50% → B2.
- **B3 router**: staging queue depth grows linearly during run AND router CPU > 80% → B3.
- **B5 lock contention**: pg_locks_sampler reports wait on pool_locks tranche dominates wait classes → B5.
- **CPU-other**: residual.

### 5.7 Output: V21_BENCHMARK_RESULTS.md

```
# v2.1 PoC Benchmark Results

Hardware: <specs>
PoC version: <git commit>
Run date: <YYYY-MM-DD>

## Summary
Peak throughput (durable, sync_commit=on): <events/sec>
Peak throughput (relaxed durability): <events/sec>
Router peak: <envelopes/sec>, avg SuperBatch size: <envelopes>, cross-SuperBatch FOR UPDATE wait count: <count>

## Throughput surface
[Table: shape × N × method × sync_commit → median events/sec ± IQR, bottleneck label]

## Latency curves
[For each shape at 25/50/75/90/100% peak load: p50, p99, p99.9 ms]

## Router metrics
[Per-shape: avg envelopes/SuperBatch, starvation force count, p99 envelope enqueue-to-route latency]

## Failure-mode recovery times
[Per failure mode: median, p99]

## GUC sweep findings
batch_size_max optimum on S2: <value>
router_window_size impact: <findings>
sync_commit on/off ratio: <x>

## Validation criteria results
C1-C4, P1-P7, R1-R3, O1-O3: PASS/FAIL with notes

Overall: PASS / CONDITIONAL PASS / FAIL
```

### 5.8 Pass criteria interpretation

The bake-off produces the surface; criteria from §4 interpret it. Conditional pass allowed if 1-2 should-pass criteria fail with documented mitigation. Must-pass failures (any of C1-C4, P1-P7, R1-R3) → PoC fails. Architecture needs rework before design-v2.1 implementation begins.

---

## 6. Implementation Milestones

**M0: Skeleton (1-2 weeks).** Extension loads, GUCs registered, shmem allocation, `_PG_init` runs (no-op router/committer). Single `poc_v21_test_hello()` SQL function.

**M1: Single-backend, single-committer (2-3 weeks).** Enqueue function works. Router stub does no packing (size-1 SuperBatches only). Single committer processes synchronously. FIFO method only. End-to-end: enqueue → status='committed', cost row visible. Validates the 2-queue pipeline mechanically.

**M2: Real methods + multi-target UNNEST (1-2 weeks).** AVG, STD methods added. Bulk UNNEST INSERTs/UPSERTs across the 6 write target tables (posting_lines, cost_layers, cost_depletions, cost_consumptions, posting_line_inventory, avg_pool_state). Validates Step 5 efficiency claim.

**M3: Router with affinity grouping (2-3 weeks).** Union-find over pool_keys to identify connected components; each component becomes a SuperBatch (split into chunks of batch_size_max for oversized components). Oldest-first dispatch by min(request_seq) within group. Head-of-line backstop for defensive multi-router future. Multi-envelope SuperBatches with mixed overlap patterns. Validates router's distinguishing primitive: same-state-key work routed together.

**M4: Multi-backend + multi-committer (1-2 weeks).** Multiple committers in worker pool, claim via CAS. Concurrent enqueue from N backends. Validates committer election and lex-lock acquisition.

**M5a: Committer death + recovery (2 weeks).** Orphan recovery via pg_xact + BGWorker slot/generation liveness check. Step 2/2.5 re-execution on takeover. Slot lifecycle correctness.

**M5b: Router death + recovery (1 week).** Boot sweep, three-state-staging-entry transitions, superbatch_id linkage check.

**M5c: Backpressure + caller-tx coupling + eject mechanism (2-3 weeks).** Synchronous-enqueue with timeout. Caller user-tx coupling via pg_xact. Two status_insert_modes implemented (caller_intx default, committer_lazy gated behind persistent_staging). The eject-and-requeue mechanism for `in_progress` user_tx_xid (no committer sleep ever). Eject_count tracking; threshold-fired failures. Includes the I-committer-non-blocking property test from §4.1 C2.

**M5d: Postmaster restart + slot audit (1-2 weeks).** Startup recovery worker. Periodic audit. All shmem-state-loss recovery paths.

**M5e: Persistent staging (durable_queue) (2 weeks).** `poc_v21_persistent_staging` table. Enqueue-path INSERT under `durable_queue=true`. Committer-side state transition (staged → completed, bundled into Step 5's bulk UNNEST). `in_shmem` state as recovery-sweep diagnostic only. Postmaster restart recovery path that replays persistent rows. Validation tests for the durability claim. Includes the `committer_lazy + persistent_staging` mode combination.

**M6: WoComplete event type (2 weeks).** Multi-pool atomic event. Snapshot mutation across multiple SKU pools + WIP pool. Per-envelope failure isolation in Step 4.

**M7: Observability (1 week).** Stats SQL functions; per-method, per-queue, per-router metrics. Bottleneck classification metric collection.

**M8: Bake-off harness (2 weeks).** Workload generators per S1-S9. Statistical runner. Output formatter. Run the full bake-off.

**M9: Validation report (1 week).** Apply criteria to measurements. Produce V21_BENCHMARK_RESULTS.md. Decide pass/fail/conditional.

Total: ~4-5 months.

---

## 7. Open Issues (PoC-specific)

Items where the spec doesn't have a definitive answer; resolution requires implementation evidence.

**[Q-A] Submission status insert mode tradeoffs.**

Two modes are implemented and measurable via `poc_v21.status_insert_mode` GUC (see §3.4):
- `caller_intx` (PoC default): status INSERT inside caller's user-tx. Cheapest. Committer's lazy fallback creates a row when it encounters an aborted user_tx_xid with no existing status row. Correct under postmaster restart because every committed caller has a durable status row.
- `committer_lazy`: no INSERT at enqueue time. Requires `poc_v21.persistent_staging=on`; the persistent staging table provides envelope durability so the recovery sweep can re-derive 'queued' state without depending on submission_status rows. Lowest enqueue overhead. The extension rejects this mode at startup if persistent_staging is off.

Bake-off measures throughput delta on the enqueue hot path between caller_intx and committer_lazy+persistent_staging. Production deployment chooses based on durability needs (durable envelopes via persistent_staging) vs absolute cheapest enqueue path (caller_intx, with status rows surviving caller commits but not aborts).

**[Q-B] Router-local starvation_tick_count: persistent or ephemeral?**

If router dies, the counts are lost; on restart, envelopes that were close to forced-as-size-1 status reset to 0. Probably fine for PoC (router-death is rare); flagged for production hardening.

**[Q-C] Lazy lock-row creation: is eager creation worth it?**

The PoC implements lazy creation with a singleton INSERT-ON-CONFLICT-DO-NOTHING loop per §1.8 Step 2. This is correct (no deadlocks) but doubles the singleton-loop SPI count compared to lock acquisition alone (2(P+Q) SPI calls per SuperBatch instead of P+Q).

Eager creation at SKU/WO setup time moves the lazy-INSERT cost off the hot path entirely: the pool_locks and wip_pool_locks rows exist before any committer needs them; Step 2 becomes just the FOR UPDATE singleton loop. Per-SuperBatch SPI cost drops from ~2-4ms to ~0.5-1ms in lock-heavy workloads.

Tradeoff: eager creation requires an out-of-band setup step (creating rows when SKUs/WOs are first registered, or via an explicit `poc_v21_ensure_pool_locks_exist(...)` SQL function called during application provisioning). For the PoC's test harness this is straightforward. For production, an unregistered SKU encountered for the first time would need a fallback (lazy creation, same singleton-loop pattern). Production deployments can run eager creation and treat lazy as the fallback safety net.

PoC measures: lazy-only baseline vs eager+lazy-fallback on shape S3 (high-overlap) and S6 (K=15). Decision deferred to the bake-off.

**[Q-D] How does committer_tx_id get assigned?**

The committer needs an XID stamped on its INSERTs and recorded in CommitterQueueEntry for orphan-recovery lookup. Options: (a) StartTransactionCommand implicitly allocates an XID on first write; query `pg_current_xact_id_if_assigned()` after the first INSERT (which is the pool_locks ON CONFLICT — guarantees XID is assigned). (b) Insert one no-op row into a dummy table immediately after StartTransactionCommand to force XID allocation deterministically. (a) is cleaner; (b) is a defensive fallback if pgrx's wrapper has edge cases. Test which works.

**[Q-E] Router window scan: from staging head only, or sliding?**

Spec says scan from head each tick. Alternative: track a cursor and slide forward, returning to head only on tick exhaustion. Sliding gives better throughput at cost of fairness; head-scan is fair but rescans unhandled entries. Spec defaults to head-scan; measure.

**[Q-F] Backend connection model for committers.**

Committers acquire SPI; they're PG backends. Two options:
- (a) Committers are dedicated BGWorkers, one per `poc_v21.committer_count` GUC. Connection pool size fixed at startup.
- (b) Committers are PG backends that callers temporarily become when they push to the queue and there's no committer.

PoC chooses (a) for clarity; flagged in case (b) is needed for capacity reasons.

**[Q-G] WIP lock acquisition: needed at all?**

Under affinity grouping (§1.5), WIP pool_keys participate in routing — two envelopes touching the same WIP pool are colocated in one SuperBatch. In well-formed workloads, no two envelopes ever target the same WIP pool (a WO is in-progress or completed; concurrent events for the same WO are an application-tier bug). Cross-SuperBatch contention on WIP locks is therefore expected to be near-zero in practice.

PoC could test without acquiring WIP locks (skip the Step 2 WIP SPI call) and rely on the workload's logical non-overlap on WIP. Saves one SPI per SuperBatch. Risky if the workload assumption ever breaks (e.g., a buggy caller emits two envelopes for the same WO).

PoC implements WIP locks as a defensive measure; measure overhead via the §5.5 `skip_wip_locks` sweep; consider dropping in production if measured throughput delta is < 5%.

**[Q-H] Spillover arena fragmentation under sustained mixed-size load.**

The freelist allocator's expected fragmentation behavior is unknown. PoC measures arena utilization over 7-day soak; if fragmentation worsens monotonically, consider slab allocator.

---

## 8. What this PoC explicitly defers to design-v2.1

Listed once, definitively:

- WAC family methods, close-hook DAG, variance routing, provisional flagging.
- BOM expansion logic (PoC uses synthetic K-component payloads; real BOM expansion is acct-domain).
- Multi-currency.
- Lots, units, identity dimensions.
- Analytical dimensions (routing_op, project, etc.).
- Multi-cost-book.
- Webhook delivery.
- User-tx coupling Options (A) PRE_COMMIT and (B) push-and-forget.
- Replication, HA.
- Multi-tenancy.
- The alternatives flagged in design-v2.1 §14 (numbered list grows over time; reference the section, not the count).
- acct schema migration.

(Persistent staging is IN scope for the PoC — see §1.9 and milestone M5e. The bake-off measures the durability/throughput tradeoff via the `durable_queue` parameter sweep in §5.5.)

All other deferred items are in scope of design-v2.1. PoC validates the substrate those depend on.

---

## Appendix A: Test harness reference

The `poc_v21_test_harness` Rust crate provides:

- **Workload generators**: produces event streams matching §5.2 shapes. Per-shape, parameterized by `g`, `K`, `N`, method_mix, event_mix.
- **Backend orchestration**: spawns N psql connections, coordinates via barriers, collects per-backend metrics.
- **Fault injection**:
  - `inject_committer_failure(kind)`: triggers tx failure, sigkill, slot abandonment at named points.
  - `inject_router_failure(kind)`: pre-push, between-push-and-cas, post-cas.
  - `corrupt_shmem(region, pattern)`: best-effort.
  - `kill_postmaster(immediate=true)`: clean restart test.
- **Invariant verification**:
  - `assert_invariants(state)`: runs all I1, I4/I5, etc. checks.
  - `assert_no_drift(snapshot_before, snapshot_after, expected_delta)`.
- **Proptest harness** with the failure-injection vocabulary.
- **Latency histogram** via hdrhistogram.
- **pg_locks_sampler** integration.
- **Router stats reader**: read per-tick metrics from shmem.

Test files organized:
- `tests/correctness/`: §3 failure-mode tests, §4.1 invariants.
- `tests/concurrency/`: multi-backend scaling tests.
- `tests/performance/`: bake-off shapes (instrumented runs).
- `tests/recovery/`: postmaster-restart, orphan-recovery scenarios.
- `tests/router/`: affinity grouping correctness, fairness, recovery.
- `tests/property/`: proptest-based randomized sequences.

---

## Appendix B: Relationship to design-v2.1

This document is the validation gate for design-v2.1. design-v2.1 describes what gets built on top once the foundation is sound.

**Cross-references from this doc to design-v2.1:**
- §8 deferred items map to design-v2.1 sections.
- §1.5 WIP lock simplification draws on design-v2.1 §2.

**Cross-references from design-v2.1 to this doc:**
- design-v2.1 §6 (router) → this doc, §1.8 lifecycle and §3.3 recovery.
- design-v2.1 §7 (committer pipeline) → this doc, §1.8 lifecycle and §2 bounds.
- design-v2.1 §14 alternatives → this doc's deferred validation.

**Update cadence:**
- This doc is mostly write-once. After PoC completion, V21_BENCHMARK_RESULTS.md is pinned; this doc becomes historical artifact.
- design-v2.1 evolves with implementation reality.

**Decision authority:**
- PoC pass authorizes design-v2.1 implementation.
- PoC fail or conditional-pass triggers design review: the v2.1 architecture may need rework, or specific failure modes need redesign before proceeding.
- PoC results inform GUC defaults, sizing recommendations, and operational guidance in design-v2.1.
