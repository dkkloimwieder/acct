# PoC Validation Specification — Queue+Committer Costing Ledger

**Status:** PoC validation spec; gate for design-v2.md construction
**Target:** PostgreSQL 18+, pgrx 0.17+
**Companion document:** `design-v2.md` (reference architecture, deferred until this PoC passes)
**Audience:** PoC implementer, reviewer deciding whether to greenlight the full build

---

## 0. Purpose and scope

This document specifies what the queue+committer architecture must demonstrate before the full system described in `design-v2.md` is worth building. It is the validation gate.

**In scope:**
- The queue+committer concurrency primitive: shard structure, slot allocation, request lifecycle, committer election, batch drain, INSERT, slot fill, waiter wake.
- Three real cost methods exercised through the queue: FIFO, weighted-average, standard cost.
- Varied workload shapes covering contention from worst-case (fan_in) to disjoint (fan_out).
- Failure modes the queue introduces: committer transaction failure, committer death mid-batch, waiter cancel, backpressure, postmaster restart, lease timeout, slot exhaustion.
- Measured throughput and latency ceilings on fixed PoC hardware.
- Theoretical analytical upper bounds for context.

**Out of scope:**
- In-transaction synchronous apply (separate path; modernized independently).
- Full trait protocol (4 required + 6 optional methods, associated-type snapshots, etc.). PoC uses minimal trait surface sufficient to run the three methods.
- WAC family (perpetual, periodic, retroactive). Close-hook DAG, variance routing, provisional flagging — all deferred to design-v2. Standard cost is the PoC's substitute for a "method that doesn't track per-row inventory."
- Posting_lines integration. PoC writes to its own minimal cost tables; full acct schema integration deferred.
- Multi-currency, lots, units, dimensions. PoC operates on `(sku, location)` keying only.
- BOM orchestration. PoC tests homogeneous batches; multi-item-per-WO orchestration deferred.

**Validation outcome:** at completion, this document carries a verdict (pass / fail / conditional pass with caveats) plus a pinned `BENCHMARK_RESULTS.md` documenting measured throughput surface, p99 latencies, and failure-mode recovery times on PoC hardware. That artifact is the input to deciding whether design-v2 construction starts.

---

## 1. The queue+committer primitive (PoC version)

### 1.1 Components

```
┌────────────────────────────────────────────────────────────┐
│  User backend (one per psql session)                       │
│  - Calls SELECT poc_ledger_apply($event_json)              │
│  - Resolves credit-side SKU's method (FIFO/AVG/STD)        │
│  - Pushes request to shmem queue (shard = hash(pool))      │
│  - Attempts committer election; loses → waits on slot      │
│  - Returns result from slot to caller                      │
└──────────────────────┬─────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────┐
│  Shmem queue (GUC-sized at startup)                        │
│  - QueueShard × N (default 256, GUC-tunable)               │
│    - Ring buffer of PendingRequest                         │
│    - Pool of ResultSlot                                    │
│    - Committer election state (CAS-based)                  │
│    - Per-shard LWLock for head/tail                        │
│  - Spillover arena for >32-id results                      │
└──────────────────────┬─────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────┐
│  Committer (one elected user backend per shard at a time)  │
│  - Waits batch_window_us OR until batch_size_max queued   │
│  - Drains shard; groups by (credit_pool, method)           │
│  - For each group: build snapshot, call method.plan_apply  │
│  - Opens committer sub-tx; INSERTs all rows                │
│  - Commits sub-tx                                          │
│  - Writes results to slots, wakes waiters                  │
│  - Releases committer role                                 │
└──────────────────────┬─────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────┐
│  PoC cost tables (minimal schema, append-only)             │
│  - poc_cost_layers     (FIFO + AVG layer events)           │
│  - poc_cost_depletions (FIFO depletions, layer-attributed) │
│  - poc_cost_consumptions (AVG + STD consumptions, no FK)   │
└────────────────────────────────────────────────────────────┘
```

### 1.2 PoC schema (minimal; not acct's schema)

```sql
-- The PoC uses its own minimal tables so the queue mechanism can be
-- exercised without full acct schema integration. design-v2 swaps these
-- for acct's posting_lines / posting_line_inventory / cost_layers etc.

CREATE TABLE poc_cost_layers (
    layer_id     BIGSERIAL PRIMARY KEY,
    sku_id       BIGINT NOT NULL,
    location_id  BIGINT NOT NULL,
    qty          BIGINT NOT NULL,            -- signed
    unit_cost    BIGINT NOT NULL,
    born_at      TIMESTAMPTZ NOT NULL,
    born_seq     BIGINT NOT NULL,            -- committer-assigned
    source_kind  TEXT NOT NULL,              -- 'receipt' | 'reversal' | 'adjustment'
    source_ref   BIGINT,
    committer_tx_id BIGINT NOT NULL,         -- for orphan recovery
    user_tx_xid  xid8 NOT NULL               -- caller's transaction; for compensation recovery
);
CREATE INDEX poc_cost_layers_pool ON poc_cost_layers (sku_id, location_id, born_at, born_seq);
CREATE INDEX poc_cost_layers_user_tx ON poc_cost_layers (user_tx_xid);

CREATE TABLE poc_cost_depletions (
    depletion_id BIGSERIAL PRIMARY KEY,
    layer_id     BIGINT NOT NULL REFERENCES poc_cost_layers(layer_id),
    qty          BIGINT NOT NULL CHECK (qty > 0),
    unit_cost    BIGINT NOT NULL,
    consumed_at  TIMESTAMPTZ NOT NULL,
    consumed_seq BIGINT NOT NULL,
    issue_id     BIGINT NOT NULL,
    method_used  TEXT NOT NULL,              -- 'fifo' | 'specific'
    committer_tx_id BIGINT NOT NULL,
    user_tx_xid  xid8 NOT NULL,
    -- A FIFO issue spans multiple layers, producing multiple rows. UNIQUE per
    -- (issue, method, layer) prevents structural duplicates on retry without
    -- breaking the legitimate multi-layer case. Idempotency for retries is
    -- enforced by committer's pre-INSERT dedup-lookup (§1.6 step 16);
    -- this constraint is the safety net.
    UNIQUE (issue_id, method_used, layer_id)
);
CREATE INDEX poc_cost_depletions_layer ON poc_cost_depletions (layer_id);
CREATE INDEX poc_cost_depletions_issue ON poc_cost_depletions (issue_id);
CREATE INDEX poc_cost_depletions_user_tx ON poc_cost_depletions (user_tx_xid);

CREATE TABLE poc_cost_consumptions (
    consumption_id BIGSERIAL PRIMARY KEY,
    sku_id         BIGINT NOT NULL,
    location_id    BIGINT NOT NULL,
    qty            BIGINT NOT NULL CHECK (qty > 0),
    applied_unit_cost BIGINT NOT NULL,
    consumed_at    TIMESTAMPTZ NOT NULL,
    consumed_seq   BIGINT NOT NULL,
    issue_id       BIGINT NOT NULL,
    method_used    TEXT NOT NULL,            -- 'avg' | 'std'
    committer_tx_id BIGINT NOT NULL,
    user_tx_xid    xid8 NOT NULL,
    -- AVG and STD produce one consumption row per issue. UNIQUE per
    -- (issue, method) catches structural duplicates; dedup-lookup is primary.
    UNIQUE (issue_id, method_used)
);
CREATE INDEX poc_cost_consumptions_pool ON poc_cost_consumptions (sku_id, location_id, consumed_at);
CREATE INDEX poc_cost_consumptions_issue ON poc_cost_consumptions (issue_id);
CREATE INDEX poc_cost_consumptions_user_tx ON poc_cost_consumptions (user_tx_xid);

-- Sketch only; Q-A in §7 leaves the choice between this table and a row-lock
-- anchor mechanism open. M3 picks based on measurement.
CREATE TABLE poc_pool_locks (
    sku_id       BIGINT NOT NULL,
    location_id  BIGINT NOT NULL,
    -- Updated (no-op self-update) by committer under FOR UPDATE to serialize
    -- committer-vs-committer for the same pool. A dummy column kept to ensure
    -- the row exists; the UPDATE is the lock semantics, not the column value.
    lock_version BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (sku_id, location_id)
);

CREATE TABLE poc_standard_costs (
    sku_id      BIGINT NOT NULL,
    location_id BIGINT NOT NULL,
    unit_cost   BIGINT NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sku_id, location_id, effective_from)
);

CREATE TABLE poc_sku_method_assignments (
    sku_id      BIGINT NOT NULL PRIMARY KEY,
    method_id   TEXT NOT NULL CHECK (method_id IN ('fifo', 'avg', 'std'))
);

-- Compensations (for user-tx-abort scenarios under compensation semantics).
CREATE TABLE poc_cost_compensations (
    compensation_id BIGSERIAL PRIMARY KEY,
    compensates_depletion   BIGINT REFERENCES poc_cost_depletions(depletion_id),
    compensates_consumption BIGINT REFERENCES poc_cost_consumptions(consumption_id),
    compensated_at  TIMESTAMPTZ NOT NULL,
    reason          TEXT NOT NULL,           -- 'user_tx_abort' | 'recovery'
    CHECK (compensates_depletion IS NOT NULL OR compensates_consumption IS NOT NULL),
    CHECK (NOT (compensates_depletion IS NOT NULL AND compensates_consumption IS NOT NULL))
);
```

**Caller contract: issue_id uniqueness.** The caller MUST generate globally-unique `issue_id` values across all pools, methods, and time. The UNIQUE constraints on cost tables provide partial enforcement of this contract:

- `poc_cost_consumptions UNIQUE (issue_id, method_used)` catches issue_id reuse across pools within AVG and STD (one consumption per (issue, method) regardless of pool).
- `poc_cost_depletions UNIQUE (issue_id, method_used, layer_id)` does NOT catch issue_id reuse across pools, because `layer_id` differs per pool. A caller reusing an issue_id for FIFO consumptions in two different (sku, location) pools would produce depletion rows that pass the constraint but violate the contract — the dedup-lookup at §1.6 step 17d would also fail to deduplicate them because each pool's batch sees different layer_ids.

This asymmetry is acceptable: issue_id-reuse-across-pools is a caller bug, not a system invariant the queue defends against. Production callers generate issue_ids from a monotone source (sequence, snowflake, UUID). For PoC testing, the workload generator allocates issue_ids from a single monotone counter, so this case doesn't arise organically; a targeted test (`test_caller_bug_issue_id_reuse_across_pools`) confirms the system continues to operate (no crash, no shmem corruption) even when the contract is violated, but does not claim to detect the violation.

### 1.3 PoC trait surface (minimal)

```rust
/// Minimal trait for PoC validation. design-v2's full protocol (close-hook
/// participation, snapshot associated type, etc.) is deferred.
pub trait PocCostMethod: Send + Sync + 'static {
    fn method_id(&self) -> &'static str;

    /// Plan a homogeneous batch (same credit_pool, same method).
    /// MUST be deterministic and pure (no SPI, no shmem mutation).
    fn plan_apply(
        &self,
        batch: &PocApplyBatch,
        snapshot: &PocSnapshot,
    ) -> PocApplyResult;
}

pub struct PocApplyBatch {
    pub pool: PocPoolKey,
    pub events: Vec<PocApplyEvent>,
}

pub struct PocPoolKey {
    pub sku_id: i64,
    pub location_id: i64,
}

pub struct PocApplyEvent {
    pub event_seq: u64,
    pub qty: i64,              // positive for consumption
    pub at: Timestamp,
    pub issue_id: i64,
}

pub struct PocSnapshot {
    pub pool: PocPoolKey,
    pub layers: Vec<PocLayerView>,        // FIFO; empty for AVG/STD if no per-layer state
    pub avg_unit_cost: Option<i64>,       // for AVG
    pub standard_cost: Option<i64>,       // for STD
    pub total_available_qty: i64,         // sum of layer effective_qty
}

pub struct PocLayerView {
    pub layer_id: i64,
    pub unit_cost: i64,
    pub effective_qty: i64,
    pub born_at: Timestamp,
    pub born_seq: i64,
}

pub struct PocApplyResult {
    pub per_event: Vec<PocEventResult>,
    pub depletion_inserts: Vec<PocDepletionRow>,    // FIFO only
    pub consumption_inserts: Vec<PocConsumptionRow>, // AVG, STD
    pub layer_inserts: Vec<PocLayerRow>,            // adjustments emitted by method (rare in PoC)
}

pub struct PocEventResult {
    pub event_seq: u64,
    pub applied_unit_cost: i64,
    pub applied_total_cost: i64,
    pub error: Option<PocError>,        // per-event errors (e.g., insufficient inventory)
}
```

The three PoC method implementations:

- **FIFO**: Walks `snapshot.layers` in `(born_at, born_seq)` order; emits one depletion row per layer touched. Spans multiple layers when qty exceeds head layer's effective_qty. Errors with `InsufficientInventory` if total_available_qty < requested.
- **AVG**: Reads `snapshot.avg_unit_cost`; emits one consumption row per event with that unit cost. Errors with `InsufficientInventory` if total_available_qty < requested (still tracks qty for the bound check).
- **STD**: Reads `snapshot.standard_cost`; emits one consumption row per event with that unit cost. Does NOT check inventory (standard cost permits negative pool qty by accounting policy).

### 1.4 Shmem layout (PoC version)

```rust
const POC_DEFAULT_SHARD_COUNT: u32 = 256;
const POC_DEFAULT_REQUESTS_PER_SHARD: u32 = 4096;
const POC_DEFAULT_SLOTS_PER_SHARD: u32 = 4096;

#[repr(C, align(64))]
pub struct PocQueueShard {
    pub lock_tranche_id: u32,
    pub _pad0: [u8; 4],

    pub head: AtomicU32,
    pub tail: AtomicU32,
    pub capacity: u32,
    pub _pad1: [u8; 4],

    pub committer_pid: AtomicI32,
    pub _pad2: [u8; 4],
    pub committer_acquired_at_ns: AtomicU64,
    pub committer_tx_seq: AtomicU64,

    pub next_request_seq: AtomicU64,
    pub next_slot_seq: AtomicU32,
    pub _pad3: [u8; 4],

    // requests[] and slots[] follow in shmem, sized at startup.
}

#[repr(C)]
pub struct PocPendingRequest {
    pub valid: AtomicU8,        // 0=empty, 1=filled, 2=abandoned, 3=in-flight
    pub _pad: [u8; 7],
    pub request_seq: u64,
    pub pool_hash: u64,         // shard-routing only; not pool identity
    pub kind: PocRequestKind,   // tagged union below
    pub backend_pid: i32,
    pub slot_idx: u32,
}

/// Tagged union for the two request types. C-repr ensures stable shmem layout.
/// Tag discriminates the active variant; the union body is u8-array-padded
/// to fixed size for fixed-stride ring storage.
#[repr(C)]
pub struct PocRequestKind {
    pub tag: u8,                // 0=Apply, 1=Compensate
    pub _pad: [u8; 7],
    pub body: PocRequestBody,
}

#[repr(C)]
pub union PocRequestBody {
    pub apply: PocApplyPayload,
    pub compensate: PocCompensatePayload,
}

#[repr(C)]
pub struct PocApplyPayload {
    pub method_tag: u8,         // 0=fifo, 1=avg, 2=std
    pub _pad: [u8; 7],
    pub event_qty: i64,
    pub event_at_micros: u64,
    pub event_issue_id: i64,
    pub event_sku_id: i64,      // pool identity
    pub event_location_id: i64, // pool identity
    pub user_tx_xid: u64,       // FullTransactionId (use pg_sys::FullTransactionId
                                // typed wrapper in actual implementation, not raw u64).
                                // Caller calls pg_sys::GetCurrentTransactionId() before
                                // push, forcing XID allocation if not yet assigned.
}

#[repr(C)]
pub struct PocCompensatePayload {
    pub original_committer_tx_id: u64,
    pub original_issue_id: i64,
    pub original_method_tag: u8,    // 0=fifo, 1=avg, 2=std (same encoding as Apply)
    pub _pad: [u8; 7],
    pub original_sku_id: i64,       // for shard routing of the compensation request
    pub original_location_id: i64,
    pub user_tx_xid: u64,           // the aborting user-tx; for compensation row attribution
}

#[repr(C, align(64))]
pub struct PocResultSlot {
    pub state: AtomicU8,        // 0=free, 1=allocated, 2=filled, 3=abandoned
    pub _pad: [u8; 7],
    pub applied_unit_cost: AtomicI64,
    pub applied_total_cost: AtomicI64,
    pub error_code: AtomicU16,
    pub _pad2: [u8; 6],
    pub depletion_count: AtomicU16,
    pub _pad3: [u8; 6],
    pub depletion_ids_inline: [AtomicI64; 32],   // fast path
    pub spillover_offset: AtomicU32,             // >32 → arena
}
```

`PocSpilloverArena`: a separate shmem region for variable-length depletion-id arrays exceeding 32 elements. Freelist-managed. Sized via GUC `poc_ledger.spillover_arena_mb` (default 64MB).

### 1.5 GUCs (PoC)

| GUC | Default | Range | Reload |
|-----|---------|-------|--------|
| `poc_ledger.shard_count` | 256 | 16-4096 power-of-two | Postmaster |
| `poc_ledger.requests_per_shard` | 4096 | 256-65536 | Postmaster |
| `poc_ledger.slots_per_shard` | 4096 | 256-65536 | Postmaster |
| `poc_ledger.spillover_arena_mb` | 64 | 1-2048 | Postmaster |
| `poc_ledger.batch_window_us` | 500 | 50-50000 | Sighup |
| `poc_ledger.batch_size_max` | 1024 | 16-65536 | Sighup |
| `poc_ledger.committer_lease_ms` | 100 | 10-10000 | Sighup |
| `poc_ledger.queue_full_timeout_ms` | 5000 | 100-60000 | Sighup |
| `poc_ledger.semantics` | `compensation` | `compensation` \| `reservation` | Sighup |

### 1.6 Request lifecycle (end-to-end)

**Semantic notes before the steps:**

*R4 in the queue+committer model.* In the in-tx synchronous model, R4 (FOR UPDATE before snapshot read) is enforced by the caller's backend. In the queue+committer model, the caller does not read pool state at all — it pushes a request and receives a result. **R4 is enforced solely at the committer**, in step 16c-d below: FOR UPDATE on the pool lock row precedes snapshot construction. Reviewers porting in-tx patterns will look for the lock in the caller's path and not find it; this is by design. The caller's path has no pool-state observation to protect.

*Push order vs. lock-acquisition order.* Request scheduling is FIFO-by-push-arrival within a shard (the ring's head/tail discipline preserves arrival order in drain). But cost-state observation is FIFO-by-committer-lock-acquisition across shards and across consecutive batches. These can diverge: a request B pushed after request A may be planned against state that includes A's commit if A's committer acquired the pool lock first. This is correct behavior — the committer is the only point of pool-state observation, and serialization by lock acquisition is the actual semantic. The push order matters only for fairness within a shard's drain.

**Caller backend:**

```
 1. Resolves method via poc_sku_method_assignments (cached per-backend).
 2. Forces user-tx XID allocation: user_tx_xid = pg_sys::GetCurrentTransactionId().
      (Without this, a read-only-so-far user-tx has no XID yet, breaking
       compensation recovery's pg_xact lookup.)
 3. Computes pool_hash = hash(sku_id, location_id).
 4. shard_idx = pool_hash & (shard_count - 1).
 5. Allocates slot OUTSIDE the shard LWLock:
      slot_seq = shard.next_slot_seq.fetch_add(1, AcqRel).
      slot_idx = slot_seq mod slots_per_shard.
      CAS slot.state from free → allocated. If CAS fails, increment fetch_add
      again (linear probe up to MAX_SLOT_PROBE = 16). If all probes fail,
      treat as slot-pressure → enter backpressure wait (§3.4) until a slot
      audit reclaims free state, then retry.
 6. LWLockAcquire(shard.lock_tranche_id, LW_EXCLUSIVE).
      (Lock held only for ring head/tail manipulation, not slot allocation.)
 7. Writes PocPendingRequest into ring at tail, including user_tx_xid.
 8. CAS tail forward.
 9. LWLockRelease.
10. CAS-attempts committer_pid 0 → MyProcPid. (Single-word CAS; lease
      acquisition timestamp updated by separate Release store immediately
      after on success — committer_acquired_at_ns. Read-side validators
      account for the brief window where pid is set but timestamp is stale
      by treating timestamp=0 as "just-acquired, no timeout possible yet.")
      Win → become committer, GOTO committer path.
      Lose → become waiter, GOTO waiter path.
```

**Committer path:**

```
11. WaitLatch(MyLatch, batch_window_us / 1000, set_at_request_arrival).
      Wakes on timeout OR batch_size_max queued OR caller-cancel.
12. LWLockAcquire(shard.lock_tranche_id, LW_EXCLUSIVE).
13. Walk ring [head, tail); for each valid request, copy out + set valid=in_flight.
14. Advance head past drained range.
15. LWLockRelease.
16. Group drained requests by (pool_hash, method_tag).
17. For each group:
      a. Resolve method handle.
      b. BeginInternalSubTransaction("poc_committer_batch").
      c. SPI: SELECT lock_version FROM poc_pool_locks
              WHERE sku_id=$1 AND location_id=$2 FOR UPDATE.
              (R4: snapshot reads in step d strictly follow this lock.)
      d. SPI: Pre-INSERT dedup-lookup. ONE query per group (not per event)
              covering all issue_ids in the group:
                SELECT issue_id, depletion_id, layer_id, qty, unit_cost
                  FROM poc_cost_depletions
                  WHERE issue_id = ANY($issue_ids::bigint[])
                    AND method_used = $method
                UNION ALL
                SELECT issue_id, consumption_id AS depletion_id, NULL, qty, applied_unit_cost
                  FROM poc_cost_consumptions
                  WHERE issue_id = ANY($issue_ids::bigint[])
                    AND method_used = $method;
              For each event in the group:
                If the query returned rows for this event's issue_id:
                  Replayed event. Aggregate the existing rows into a result
                  (sum qty, weighted-avg unit_cost for FIFO; single row for
                  AVG/STD), use as this event's result, skip plan_apply.
                Else:
                  Include this event in the plan_apply batch.
      e. SPI: build PocSnapshot (read layers / avg / std for events that
              were NOT skipped by dedup).
      f. Call method.plan_apply(batch_filtered, snapshot) → result.
      g. SPI: INSERT all layer_inserts, depletion_inserts, consumption_inserts.
              Assign born_seq / consumed_seq from per-shard counters.
              Stamp committer_tx_id (from shard.committer_tx_seq) and
              user_tx_xid (from each request's user_tx_xid) on every row.
      h. Commit sub-tx. On serialization_failure: rollback, retry once;
              second failure → all slots in group receive error.
              On UNIQUE violation (constraint catches a dedup-logic bug):
              rollback, log diagnostic, fail batch — UNIQUE never triggers
              on correctness path; if it triggers, it's a bug.
18. For each drained request: write result into slot
      (combining plan_apply outputs and dedup-replayed results).
      Atomically set state=filled.
19. SetLatch on each waiter's PGPROC.
20. CAS committer_pid back to 0.
21. Return result for the committer's own request from its own slot.
```

**Waiter path:**

```
22. ProcWaitForSignal(slot.state == filled || canceled).
23. CHECK_FOR_INTERRUPTS loop with WL_TIMEOUT 100ms.
24. On wake:
      slot.state == filled → read result, return to caller.
      slot.state == abandoned → return queue_recovered error; caller retries
                                  with same issue_id; dedup-lookup ensures
                                  correctness.
      Canceled → atomically CAS slot.state to abandoned; return cancel error.
```

### 1.7 Semantics (compensation default)

After the committer sub-tx commits, `committer_tx_id` is stamped on every inserted row. The caller's user-tx is independent. If the caller's tx aborts:

```
XactCallback(XACT_EVENT_ABORT) in the caller backend:
  - For each (committer_tx_id, issue_id, method, sku_id, location_id) observed
    during this user-tx (tracked in a per-backend list populated when each
    poc_ledger_apply call returns):
    - Push a PocPendingRequest with kind.tag = Compensate, body.compensate
      filled with PocCompensatePayload (§1.4) targeting the SHARD of the
      original pool (hash(sku_id, location_id) — same shard as the original
      apply, ensuring serialization with concurrent applies to the same pool).
  - The next committer tick for that shard processes Compensate-kind requests:
    - For Apply requests (kind.tag == 0): per §1.6 step 17.
    - For Compensate requests (kind.tag == 1):
      - Look up the original rows by (issue_id, method, committer_tx_id):
          SELECT depletion_id FROM poc_cost_depletions
            WHERE issue_id = $1 AND method_used = $2
              AND committer_tx_id = $3
          UNION ALL
          SELECT consumption_id FROM poc_cost_consumptions
            WHERE issue_id = $1 AND method_used = $2
              AND committer_tx_id = $3;
      - For FIFO: one compensation row per matching depletion_id. A FIFO
        apply that spanned 32 layers produces 32 depletion rows and the
        compensation produces 32 compensation rows (1:1 by depletion).
      - For AVG/STD: one compensation row per matching consumption_id (1:1).
      - INSERT into poc_cost_compensations under the committer sub-tx.
      - On commit, the original rows remain in their tables (append-only);
        the compensation rows offset them for effective_qty derivation.
```

The shard routing of compensations to the original pool's shard means a single committer in that shard sees both fresh applies and compensations interleaved. The committer's drain loop dispatches per kind:

```
For each drained request:
  match request.kind.tag:
    0 (Apply)      → §1.6 step 17 (dedup-lookup, plan_apply, INSERT cost rows)
    1 (Compensate) → above (lookup originals, INSERT compensation rows)
```

Both kinds run within the same committer sub-tx if grouped together (Apply requests grouped by (pool, method); Compensate requests grouped by pool only). On any sub-tx failure, both kinds receive the same error and waiters retry / abort uniformly.

**Reservation semantics** (opt-in via GUC `poc_ledger.semantics = 'reservation'`): committer holds capacity in `poc_cost_reservations` (separate table, defined in §1.2) and the depletion/consumption row is INSERTed only at caller's pre-commit XactCallback. PoC implements both code paths but defaults to compensation. Reservation is exercised in correctness tests but NOT measured in primary bake-off surface; it's the alternative-path-exists demonstration.

### 1.8 What the PoC explicitly does NOT do

- No close-hook framework. No period boundaries. No variance routing.
- No provisional flagging.
- No WAC methods. AVG is a "weighted-average-running" method, not WAC perpetual/periodic/retroactive (those have close-hook complexity beyond PoC scope).
- No BOM orchestration. Each apply call is one (item, qty) event; batches are homogeneous per pool.
- No lots, units, dimensions, currencies. Only `(sku_id, location_id)` keying.
- No replica/HA. PoC runs on a single primary. Replication out of scope.

---

## 2. Theoretical upper bounds

Established analytically before measurement, to provide context for the measured numbers. If measured numbers fall far below analytical bounds, the gap localizes where the architecture loses efficiency.

### 2.1 Bottlenecks, in order of expected impact

**B1: WAL fsync per committer-tx commit (GLOBAL, not per-shard).** Each committer batch produces one COMMIT and one fsync (assuming `synchronous_commit = on`). WAL is a single global resource; concurrent committers across shards serialize at WAL flush. For typical NVMe with `wal_sync_method = fdatasync`, fsync latency is ~50-200μs depending on hardware. **Global ceiling**: ~5K-20K commits/sec aggregate across all shards.

The aggregate event throughput from B1 is therefore `global_fsync_rate × avg_events_per_batch`. Example: 10K commits/sec × 100 events/batch = 1M events/sec WAL-ceiling. 10K commits/sec × 10 events/batch = 100K events/sec. **Batch size is the lever**: higher `batch_window_us` (more time to accumulate) and higher `batch_size_max` push the WAL ceiling up by amortizing fsyncs across more events.

With `synchronous_commit = off`: WAL writes are buffered, durability is relaxed; ceiling moves to WAL write rate, typically ~50K-200K commits/sec globally. PoC bake-off measures both; relaxed durability case characterizes the SPI-bound regime without WAL confounding it.

**B2: SPI overhead per batch (per-committer, parallel across shards).** Each committer batch performs:
- 1 SPI for FOR UPDATE on pool lock row.
- 1 SPI for pre-INSERT dedup-lookup (covers all events in group via WHERE issue_id IN (...)).
- 1-3 SPI for snapshot construction (FIFO reads layers; AVG reads aggregate; STD reads standard cost).
- 1 SPI per INSERT (one for layers, one for depletions, one for consumptions; INSERT batches multiple rows per call).

Total: 4-7 SPI calls per batch, each ~10-50μs depending on plan complexity. Per-committer ceiling: ~2-8K batches/sec when SPI-bound. Multiple committers run in parallel across shards, so aggregate is per-shard ceiling × active-shard-count, subject to B1 global cap.

**B3: LWLock contention on shard's tranche entry (per-shard).** Push path holds the LWLock briefly (~1μs) to update head/tail; with slot allocation moved outside the LWLock (§1.6 step 5), the critical section is just the ring write. Under heavy concurrent push to one shard, contention scales with backend count. At 32 backends pushing to one shard, expected LWLock wait time is 2-20μs per push. Per-shard ceiling: ~50-500K pushes/sec.

**B4: Committer election CAS contention.** Negligible per analysis in v1. CAS itself is ~10ns; lease time keeps elections to ~10/sec/shard.

**B5: Waiter wake latency.** SetLatch + ProcSignal: typically ~2-10μs per waiter. For a batch of N waiters, total wake time scales with N. At 1000 waiters per batch: 2-10ms wake time. Caps p99 latency of the last-woken waiter; doesn't cap throughput directly.

**B6: Result slot writeback.** Per-event: a few atomic stores, ~50ns per event. Negligible.

### 2.2 Combined theoretical ceiling

**Global B1 ceiling**: `fsync_rate × avg_batch_size`. For PoC hardware (NVMe), typical:
- `sync_commit=on`, batch=100: ~10K × 100 = ~1M events/sec WAL-ceiling.
- `sync_commit=on`, batch=10:  ~10K × 10  = ~100K events/sec WAL-ceiling.
- `sync_commit=off`, batch=100: ~100K × 100 = ~10M events/sec (SPI/CPU becomes binding).

**Per-shard B2 ceiling**: ~2-8K batches/sec, parallel across active shards, capped by B1 globally.

**Realistic aggregate on PoC hardware: 30-100K events/sec.** This is well below both ceilings — meaning the binding constraint at moderate batch sizes is neither pure WAL nor pure SPI alone, but their interaction with concurrent contention, kernel scheduling, buffer pool pressure, and lock manager overhead. The bake-off measures actual; analytical bounds tell us *where* to look if measured falls short.

The "binding bottleneck" classification (§5.7) records, per cell, which of B1/B2/B3/B5/CPU-other was dominant. This makes the surface interpretable beyond raw numbers.

### 2.3 Where the model breaks down

The analytical bounds assume:
- Disjoint workload across shards (no hash collisions on hot pools).
- Steady-state operation (no committer election thrash, no slot allocation contention).
- Single method per shard at a time (mixed-method batches add overhead in dispatch).
- No failure recovery happening concurrently.

Production-like workloads violate all of these. The measured upper bound (§5) is what matters; analytical bounds tell us *where* to look if measured falls short.

---

## 3. Failure modes

Each failure mode has: trigger, detection, recovery mechanism, test that proves recovery, and whether it's PoC-must-pass or hardening-phase.

### 3.1 Committer-tx failure (constraint violation, deadlock, OOM)

**Trigger:** committer's sub-tx raises during INSERT (e.g., deferred constraint catches over-consumption, deadlock with concurrent vacuum, OOM in plan cache).

**Detection:** `BeginInternalSubTransaction` + `PgTryBuilder` catches the ERROR.

**Recovery:**
- Sub-tx rolls back (PG semantics).
- For each slot in the batch: write error state (with error code matching the failure class).
- Wake waiters; they return error to callers.
- Caller's user-tx receives error from its `poc_ledger_apply` call; can retry or abort its own tx.
- Critical: if the failure was a serialization conflict on the pool lock row, retry the batch once before failing (catches transient deadlocks).

**Test:** `test_committer_tx_constraint_violation` and `test_committer_tx_deadlock`. Inject the failure via test-only error-injection points. Assert:
- All slots in the batch receive error.
- No rows persist from the batch.
- Pool state unchanged.
- Next batch (on same shard, after failure) succeeds normally.

**Status:** PoC-must-pass.

### 3.2 Committer backend death mid-batch

**Trigger:** committer backend receives SIGKILL (or crashes) between BeginInternalSubTransaction and CommitInternalSubTransaction. PG's normal cleanup runs.

**Detection:** the dead backend's committer_pid remains in the shard's `committer_pid` field with stale `committer_acquired_at_ns`. Next backend to push to that shard checks lease:
```
if (now_ns - committer_acquired_at) > lease_ms_to_ns
   && pg_pid_alive(committer_pid) == false:
    attempt CAS committer_pid -> MyProcPid (stale takeover)
```

**Recovery:**
- New committer wakes; before processing fresh requests, runs `recover_orphaned_shard`:
  - Scan ring [head, tail) for `valid == in_flight` requests (the dead committer marked them in-flight but didn't fill their slots).
  - For each in-flight request:
    - Look up the dead committer's `committer_tx_id` (stored in shard state, retained across committer changes).
    - Query pg_xact for that tx's status.
    - **If committed**: results are durable in tables. Backfill the slot:
        - SPI: `SELECT applied_unit_cost, applied_total_cost FROM poc_cost_depletions WHERE issue_id = $1 AND committer_tx_id = $2` (or `poc_cost_consumptions` for AVG/STD).
        - Aggregate per-issue, write into slot, mark state=filled, wake waiter.
    - **If aborted**: results are gone. Mark slot abandoned; waiter retries.
    - **If still in-progress**: poll pg_xact status with exponential backoff (10ms, 20ms, 40ms, 80ms... up to lease_ms × 10). If still in-progress after the bound, treat as abandoned. The pg_xact update should be atomic with the sub-tx commit but `pg_xact_status` queries can lag briefly; the bounded poll handles this without a magic constant.
- Once orphan recovery completes, new committer processes fresh requests normally.

**Detection primitive: pg_pid_alive.** PG doesn't expose this as an SQL function. The implementation uses libc::kill(committer_pid, 0) via FFI from the extension's C code — signal 0 checks existence without actually signaling. Works on Linux/macOS. ESRCH return → dead; 0 return → alive; EPERM → alive (different user; doesn't happen for same-cluster backends).

**Test:** `test_committer_sigkill_pre_commit` and `test_committer_sigkill_post_commit`. The harness spawns a helper backend, has it begin a batch, then SIGKILL it at the specific point. Assert:
- For pre-commit kill: orphan recovery marks slots abandoned; waiters retry; no rows in cost tables for this batch's issue_ids.
- For post-commit kill: orphan recovery backfills slots from tables; waiters return correct results; rows in cost tables are consistent.
- In both cases, lease takeover happens within 2× committer_lease_ms.
- Pool state is internally consistent (no over-consumption, no duplicate depletions).

**Eventual-resolution invariant (proptest):** for any sequence of kills, cancels, restarts, and concurrent applies, every slot allocated by any backend eventually reaches state ∈ {filled, abandoned} within `MAX_RESOLUTION_BOUND = 10 × committer_lease_ms` of the failure event. No slot remains stuck in `in_flight` or `allocated` indefinitely. This is added to the C2 invariant set (§4.1).

**Status:** PoC-must-pass. The post-commit kill case is the gnarliest single path in the design; thorough test required.

### 3.3 Caller (waiter) cancel mid-wait

**Trigger:** user backend receives query cancel (statement_timeout, pg_cancel_backend, client disconnect) while waiting on its slot.

**Detection:** `CHECK_FOR_INTERRUPTS` in the waiter's wait loop raises `ERROR (canceled)`.

**Recovery:**
- Waiter atomically CAS-es its slot state from `allocated` → `abandoned`.
- Waiter raises cancel error to caller.
- Committer, when processing the batch containing this request:
  - For **idempotent methods** (issue_id is the natural idempotency key, plan_apply is deterministic): processes the request anyway, INSERTs row(s) with the abandoned slot's data. Result is durable; no waiter reads it. If the caller's user-tx is aborting (which it usually is on cancel), normal compensation handles cleanup.
  - For **non-idempotent or reservation-mode requests**: skip the request entirely. Slot stays abandoned. (PoC's three methods are all idempotent given a stable issue_id, so the "process anyway" path is what runs.)

**Test:** `test_waiter_cancel_mid_wait_processes` and `test_waiter_cancel_after_commit_compensates`.
- Cancel mid-wait, before committer drains: assert request is still processed; row inserted; cancel error returned to caller; compensation enqueued by caller's abort callback.
- Cancel after committer commits but before slot reads: assert result is durable; waiter doesn't deadlock; cancel error returns; compensation enqueued.

**Status:** PoC-must-pass.

### 3.4 Backpressure (queue full)

**Trigger:** shard's ring buffer is full (`(tail - head) >= capacity - 1`). New push attempt finds no free slot.

**Detection:** push path checks ring fullness under the shard's LWLock before reserving a slot.

**Recovery:**
- Push blocks on a condition variable attached to the shard.
- Committer's drain notifies the cv after advancing head.
- `CHECK_FOR_INTERRUPTS` periodically (every 100ms) so cancel works.
- Bounded by `queue_full_timeout_ms`. On timeout, returns `queue_full` error.
- Caller's user-tx aborts or retries.

**Test:** `test_backpressure_blocks_then_unblocks` and `test_backpressure_timeout`.
- Fill a shard's ring; assert next push blocks.
- Drain one request; assert blocked push completes.
- Fill a shard's ring; sleep until queue_full_timeout_ms exceeded; assert push returns `queue_full` error.

**Status:** PoC-must-pass.

### 3.5 Postmaster restart (crash recovery)

**Trigger:** postmaster restart, planned or otherwise. All shmem state is lost.

**Detection:** on `_PG_init`, shmem is allocated fresh, queue state is empty.

**Recovery:**
- A startup-phase recovery worker runs before the queue accepts new requests:
  - Reads the maximum `committer_tx_id` from cost tables.
  - Initializes per-shard `committer_tx_seq` counters from that watermark.
  - Re-bootstraps shmem-only state (queue counters, slot freelists).
- For in-flight user transactions whose XactCallback never fired (because the backend died with the postmaster):
  - Scan cost tables for `user_tx_xid` values (denormalized on every cost row).
  - For each distinct user_tx_xid: query pg_xact for status.
    - Status `committed`: no action needed; the user-tx and the cost rows are both durable.
    - Status `aborted` or `unknown` (assumed aborted after PG's TX cleanup at restart): emit compensations for every cost row stamped with this user_tx_xid.
  - This is a one-shot scan at startup, parallelizable across user_tx_xid groups; ongoing operation does not need it.
  - **Scope limitation:** scan is bounded by tables seeded for the PoC's test/load runs. For production at acct scale, an incremental tracking mechanism (last-recon-watermark) is needed; deferred to design-v2 (Q-F in §7).
- After recovery completes, queue accepts new requests.

**Test:** `test_postmaster_restart_recovers_in_flight` and `test_postmaster_restart_no_data_loss`.
- Start a load run; mid-load, kill postmaster (`pg_ctl stop -m immediate`).
- Restart postmaster.
- Assert:
  - All committed cost rows are intact (durability via WAL).
  - No shmem state needed to interpret table state.
  - Resumed load run continues without consistency violations.
  - Compensations for unfinished user-txs are correctly emitted.

**Status:** PoC-must-pass.

### 3.6 Lease timeout false positives

**Trigger:** a committer is doing legitimate slow work (e.g., a method's plan_apply takes longer than usual under load) and its lease expires while still processing.

**Detection:** another backend attempts CAS takeover, sees stale lease, would steal.

**Recovery:**
- Before stealing, the contending backend verifies `pg_pid_alive(stored_committer_pid)`. If alive, the contender backs off: re-reads `committer_acquired_at_ns`, sleeps `lease_ms`, retries.
- This means lease timeout isn't sufficient for takeover; it must be accompanied by evidence the committer is actually dead.
- Cost: a legitimately-slow committer holding the role for longer than lease_ms blocks the shard. Mitigation: shrink batches via `batch_size_max` so per-batch time stays well below `lease_ms` even on slow plan_apply.

**Test:** `test_slow_committer_not_stolen` and `test_dead_committer_stolen`.
- Slow: inject a sleep in plan_apply to make a batch take 2× lease_ms; assert no takeover happens; committer eventually finishes and proceeds normally.
- Dead: kill committer; assert takeover happens within ~2× lease_ms.

**Status:** PoC-must-pass.

### 3.7 Slot exhaustion / leak

**Trigger:** slots allocated but never freed (e.g., abandoned slots not cleaned up; abandoned-then-processed slots whose state transition is incomplete).

**Detection:** slot allocator finds no free slot; push enters retry loop; eventually times out.

**Recovery:**
- Slots are freed by:
  - Filled slot read by its waiter → freed by waiter.
  - Abandoned slot, processed by committer → freed by committer (idempotent method case).
  - Abandoned slot, not processed (reservation mode) → freed by committer immediately on observation.
- Periodic shmem audit (every 10s) scans slots; any slot in `allocated` state for > 60s with no associated live backend (verify via `pg_pid_alive`) is force-freed.

**Test:** `test_slot_leak_recovered` — manufactured leak (kill a backend mid-allocation); assert the audit reclaims within 70s.

**Status:** PoC-should-pass (the periodic audit is a safety net; if no leaks observed in 7-day soak test, the audit code path is uncovered but the absence of leaks is the success criterion).

### 3.8 Shmem corruption (best-effort)

**Trigger:** a bug or external memory corruption invalidates an atomic, ring buffer pointer, or slot state.

**Detection:** committer or waiter encounters impossible state (head > tail, slot state value out of range, depletion_count > 32 with no spillover).

**Recovery:**
- Detected impossible states → log + PANIC (postmaster-wide restart). Better to crash than continue with corrupted state.
- Postmaster restart triggers §3.5 recovery; cost tables remain intact.

**Test:** `test_shmem_corruption_detected` — write garbage to shmem via the test harness (requires direct shmem access; PoC harness includes a test-only function for this), assert detection + PANIC + clean recovery on restart.

**Status:** Best-effort. We attempt to construct corruption scenarios but accept that exhaustive coverage isn't feasible. The goal is "the detection paths exist and trigger on corruption we can synthetically introduce" — not "all possible corruption is detected."

### 3.9 Spillover arena exhaustion

**Trigger:** the spillover arena fills up due to many concurrent results with >32 depletions each.

**Detection:** arena allocator returns `out_of_arena` to the committer.

**Recovery:**
- Committer cannot complete the batch as planned. Falls back: rolls back sub-tx; writes `result_too_large` error to all slots in the batch; wakes waiters; caller's tx aborts or retries with smaller batches.
- Operational: monitor arena utilization; if regularly approaching capacity, bump `poc_ledger.spillover_arena_mb`.

**Slot lifecycle coupling:** when a slot is freed (waiter reads filled result, slot-leak audit reclaims abandoned slot, etc.), any spillover-arena block referenced by that slot's `spillover_offset` is also freed back to the arena freelist atomically with the slot transition to `free`. This is the only path that returns blocks to the arena; M5c must implement the coupling explicitly. A slot freed without releasing its spillover block leaks arena capacity and §3.7's audit cannot recover it.

**Test:** `test_spillover_arena_exhaustion` — undersized arena, large-fan_in workload; assert clean error propagation and recovery.

**Status:** PoC-must-pass.

### 3.10 Method-level errors propagating cleanly

**Trigger:** method.plan_apply returns a per-event error (e.g., `InsufficientInventory`).

**Detection:** per-event error in `PocApplyResult.per_event[].error`.

**Recovery (explicit per-event flow):**

Per-event partial success requires careful sequencing so per-event slot writes correspond to the right events. Three event categories result from §1.6 step 17d and step 17f:

- **Replayed events** — already returned rows from the batched dedup-lookup; result computed from existing rows. No plan_apply, no INSERT.
- **plan_apply-success events** — survived dedup-lookup, ran through plan_apply, returned `error.is_none()`. Contribute rows to layer_inserts / depletion_inserts / consumption_inserts.
- **plan_apply-error events** — survived dedup-lookup, ran through plan_apply, returned `error.is_some()`. No rows contributed; per-event error returned.

The committer's step 17-18 expansion:

```
1. After dedup-lookup (17d), partition the group's events into:
     replayed_events  : returned rows from dedup-lookup; result already computed.
     to_plan          : not found in dedup-lookup; will go through plan_apply.
2. Build PocSnapshot for to_plan events (17e).
3. Call method.plan_apply(to_plan, snapshot) → result with per_event[] array.
4. Partition result.per_event entries into:
     success_events: error.is_none() → contribute rows.
     error_events:   error.is_some() → no rows.
5. INSERT all rows from success_events (within committer sub-tx).
6. Commit sub-tx.
7. For each event in ORIGINAL batch order (preserving caller arrival order
   for waiter wake fairness):
     - If event in replayed_events: write slot with the result computed
       from dedup-lookup rows. State = filled.
     - If event in success_events: write slot with plan_apply output
       (applied_unit_cost, applied_total_cost, depletion_ids / consumption_id).
       State = filled.
     - If event in error_events: write error code to slot. State = filled.
       (filled-with-error is terminal; state machine doesn't distinguish.)
8. Wake all waiters (one SetLatch per slot).
```

Critical: per-event errors do NOT roll back the sub-tx. The sub-tx commits with only success_events' rows; replayed_events contribute no new rows (their rows already exist from the prior attempt); error_events return to their callers as per-event errors without affecting others in the batch.

If the sub-tx itself fails (deadlock, OOM — §3.1), all events in the batch — including replayed_events — receive the sub-tx-failure error in their slots. Replayed events fail at the batch level even though their rows are already durable; the caller retries, the retry's dedup-lookup hits again, and result is returned without another sub-tx. Per-event errors, replay-bypass, and batch-level errors are distinct error codes.

**Test:** `test_per_event_error_partial_success` — batch with mix of valid and over-consuming requests; assert valid ones succeed, invalid ones return per-event error, sub-tx committed (only success rows visible). Plus `test_mixed_replay_and_fresh_in_one_batch` — batch contains some events from a prior canceled attempt (replay-eligible) and some new events; assert correct sorting into categories and correct slot writes for each.

**Status:** PoC-must-pass.

---

## 4. Validation criteria (the gate)

Each criterion is a pass/fail bar. PoC is a pass only if all "must-pass" criteria pass. Conditional pass is allowed if 1-2 "should-pass" criteria fail with documented mitigations.

### 4.1 Correctness criteria (must-pass)

**C1. All §3 PoC-must-pass failure modes recover correctly.** Each test in §3 with that status passes. Specifically: committer-tx failure (§3.1), committer death pre/post commit (§3.2), waiter cancel (§3.3), backpressure (§3.4), postmaster restart (§3.5), lease timeout false-positive (§3.6), per-event error propagation (§3.10).

**C2. Invariants hold under property-based testing.** A `proptest` harness generates random sequences of (apply, abort, cancel, kill, restart) operations across multiple backends and methods. After every step, these invariants hold:
- **I1**: For every depletion, the referenced layer group's effective_qty ≥ depletion.qty.
- **I4/I5**: `consumed_seq` monotone within `(layer_id, consumed_at)`; `born_seq` monotone within `(sku_id, location_id, born_at)`.
- **I-row-unique**: No structural duplicate. `UNIQUE (issue_id, method_used, layer_id)` on depletions never triggers in correctness path (only as safety net for dedup-logic bugs). If proptest triggers the constraint, that's a bug-finding outcome — the test reports it as a failure with the offending sequence, and the dedup-lookup logic is debugged. Do NOT suppress UNIQUE-violation errors in proptest with retry-on-23505; let them surface as test failures.
- **I-compensation-coverage**: every aborted user-tx (verified by `user_tx_xid` having pg_xact status 'aborted') has matching compensation rows for every cost row stamped with that user_tx_xid.
- **I-row-attribution**: every cost row has a valid `committer_tx_id` and `user_tx_xid`.
- **I-replay-idempotent**: retry of a canceled apply call with the same `issue_id` and `method_used` produces rows-identical-to-first-attempt result (same applied_unit_cost, same depletion_ids). No duplicate INSERT (dedup-lookup hits). UNIQUE never triggers in correctness path.
- **I-eventual-resolution**: every slot allocated by any backend reaches state ∈ {filled, filled-with-error, abandoned} within `MAX_RESOLUTION_BOUND = 10 × committer_lease_ms` of any failure event affecting it. No slot remains stuck in `in_flight` or `allocated` indefinitely.

**C3. Determinism.** For a fixed event stream and seed (no kills/cancels), the rows written to cost tables are identical across runs. (Verified via test harness that replays a recorded event sequence and compares row-by-row.) Determinism does NOT hold under non-deterministic failure injection; that case is covered by C2's idempotency invariant instead.

**C4. Idempotency under retry.** A caller retrying a canceled apply call with the same `issue_id` produces no duplicate rows. The dedup-lookup at committer step 16d (§1.6) hits the prior attempt's rows and returns identical result without re-INSERTing. C4 is verified by `test_replay_returns_same_result`: cancel mid-batch; retry; assert row count unchanged in cost tables; assert returned result identical to what would have been returned had the first attempt completed; assert UNIQUE constraint never triggered.

### 4.2 Performance criteria (must-pass)

**P1. Disjoint workload scales linearly with backend count up to N.** Run fan_out (g=5000 SKUs, 32 backends each pinned to disjoint SKU subsets). Throughput at N=32 is ≥ 24× throughput at N=1 (allowing 25% efficiency loss for shared infrastructure). If achieved, the queue itself is not the bottleneck for disjoint workloads.

**P2. Same-pool workload serializes correctly.** Run fan_in (g=1, all backends consuming from one pool). Throughput at N=32 should be within 2× of N=1 (a single committer serializes the work; throughput cap is single-shard's batching efficiency). Critical: no consistency violations, no SSI conflicts surfacing as errors.

**P3. p99 latency under fan_out at moderate load.** With 16 backends and fan_out workload at 50% of measured peak throughput, p99 apply latency < 50ms. At measured peak, p99 is expected to be much higher (5-10× isn't unreasonable for queue+committer at the saturation edge). BENCHMARK_RESULTS publishes the **full latency curve** (p50, p99, p99.9 vs load %) for each shape — not just one operating point. The 50ms-at-50%-peak bar is one criterion; the curve enables operators to choose their own operating point with informed latency expectations.

**P4. Mixed-method workload doesn't pathologically degrade.** Workload with 33% FIFO, 33% AVG, 33% STD, across mixed pools. Throughput within 30% of best single-method workload on equivalent shape. If worse, method dispatch overhead is excessive.

### 4.3 Operational criteria (should-pass)

**O1. 7-day soak test at 50% of measured peak.** Run continuously for 7 days with mixed workload. Pass criteria:
- No slot leaks (audit reports 0 reclaims).
- No memory leaks (shmem high-water-mark stable within ±5%).
- No throughput degradation > 10% over the run.
- No unexplained errors.

**O2. Recovery time SLAs.**
- Lease takeover on committer death: < 2 × committer_lease_ms p99.
- Backpressure recovery on drain: < 10ms p99.
- Postmaster restart with 100K in-flight events: < 30s.

**O3. Observability metrics expose useful state.**
- Per-shard depth, committer occupancy, error counts queryable via `poc_ledger_shard_stats()`.
- Per-method dispatch counts, error rates via `poc_ledger_method_stats()`.
- Recovery events (lease takeovers, abandoned slots, compensations) counted.

### 4.4 Hardening-phase criteria (deferred from must-pass)

**H1. Shmem corruption detection (§3.8).** Best-effort coverage; not blocking PoC pass.
**H2. Spillover arena exhaustion under pathological load (§3.9).** Verified under contrived test; production tuning of arena size deferred.
**H3. Long-running compensation chains.** A user-tx that issues 1000 apply calls and aborts produces 1000 compensations. Verify performance acceptable. Deferred unless C2 reveals issues.

---

## 5. Bake-off methodology

The bake-off is the measurement protocol that produces the throughput-and-latency surface for the PoC's three methods across the standard workload shapes. Outputs feed `BENCHMARK_RESULTS.md`.

### 5.1 Hardware (fixed for PoC)

Document concrete specs in `BENCHMARK_RESULTS.md` when run. The spec assumes a single-machine PG primary with NVMe storage, ≥16 CPU cores, sufficient RAM to keep working set in buffer pool. Specific numbers from analytical bounds (§2) calibrate to this hardware.

### 5.2 Workload shapes

Each shape parameterized by:
- `g`: number of distinct SKUs (controls fan-out)
- `b`: batch size per apply call (1 for the PoC; multi-item batches deferred)
- `N`: concurrent backend count
- Method mix: FIFO-only, AVG-only, STD-only, or mixed-33-33-33

**Shape 1: fan_in (g=1).** All N backends consuming from one SKU. Worst-case contention. Each backend issues random qty consumptions at high rate.

**Shape 2: fan_out (g=5000).** N backends each pinned to a disjoint subset of 5000 SKUs. Disjoint workload. Tests linear scaling.

**Shape 3: balanced (g=50).** N backends sharing 50 SKUs uniformly. Moderate sharing. Tests committer batching efficiency under partial concurrency.

**Shape 4: zipfian (g=1000, alpha=1.0).** N backends sampling SKUs zipfian. Realistic hot-pool pattern. Tests behavior when one pool gets 10× more traffic than another within the same shard distribution.

**Shape 5: small-batch (b=100, g=50).** N backends each issuing 100 events in quick succession to ~50 SKUs. Tests committer batch window efficacy; many small ops should coalesce.

**Shape 6: mixed-method.** Shape 3 (balanced) with method assignments split 33% FIFO, 33% AVG, 33% STD. Tests dispatch overhead under mixed workload.

### 5.3 Run methodology (per acct-8hv2)

For each (shape, backend count, method mix) cell:

1. **Setup** (~30s): seed cost tables with starting inventory (10 layers per SKU, varied unit_cost). Reset shmem state. Warm caches with light pre-load.
2. **5 runs of 60s each.** Concurrent backends pinned via taskset to specific cores. Each backend runs as a tight loop: `SELECT poc_ledger_apply($event)`. Between runs, 30s rest to let buffers settle.
3. **Per-run measurements:**
   - Throughput: events/sec (committed events / wall time).
   - Latency histogram: p50, p99, p99.9, max.
   - `pg_locks_sampler` at 100ms intervals (records lock waits).
   - Per-shard queue depth samples at 1s intervals.
   - Committer election count, lease takeover count, backpressure trigger count.
4. **Statistical analysis:** median + IQR (interquartile range) across the 5 runs. IQR > 10% of median flags noise; cell is re-run or marked unreliable.

### 5.4 Backend count sweep

Per shape: N ∈ {1, 2, 4, 8, 16, 32, 64, 128, 256} where N is the number of concurrent psql client connections each running the apply loop (not PG worker processes; not parallel-query workers). The point of the sweep is to find where each shape saturates.

### 5.5 GUC sweep (limited)

Three GUC settings are swept on shape 2 (fan_out) and shape 5 (small-batch):

- `batch_window_us`: 100, 500, 2000
- `batch_size_max`: 64, 1024, 16384
- `synchronous_commit`: on, off. **off voids durability** — a postmaster crash can lose committed cost rows. BENCHMARK_RESULTS prominently notes this on the sync_commit=off peak number; it characterizes the SPI/CPU-bound regime without WAL as confounder, not a recommended production setting.

This sweep is exploratory; not all combinations measured. Goal: identify the rough optimum and confirm the parameters expose meaningful tuning.

### 5.6 Output: `BENCHMARK_RESULTS.md`

Pinned artifact, format:

```
# PoC Benchmark Results

Hardware: <CPU, RAM, NVMe specs, PG version>
PoC version: <git commit>
Run date: <YYYY-MM-DD>

## Summary

Peak throughput observed (durable, synchronous_commit=on):
  <events/sec> (shape: fan_out, N=32, method=FIFO)

Peak throughput observed (RELAXED DURABILITY, synchronous_commit=off,
  not recommended for production):
  <events/sec> (shape: fan_out, N=32, method=FIFO)

Ratio: <2-10x typical>; the gap characterizes WAL fsync as a binding
constraint in durable mode.

## Throughput surface

[Table: shape × N × method × sync_commit → median events/sec ± IQR,
  binding bottleneck label]

## Latency curve (full)

For each cell: p50, p99, p99.9 ms at 25%, 50%, 75%, 90%, 100% of cell's
peak throughput. Operators choose operating point with informed tradeoffs.

## Failure-mode recovery times

[Table: failure-mode → median recovery time, p99 recovery time, status]

## GUC sweep findings

batch_window_us optimum on fan_out: <value>
batch_size_max optimum on small-batch: <value>
sync_commit on/off throughput ratio: <number>x

## Bottleneck classification per cell

[Table: each cell labeled with binding bottleneck from
  {B1 WAL, B2 SPI, B3 LWLock, B5 wake, CPU-other}]

## Validation criteria results

C1: PASS / FAIL (with notes)
C2: PASS / FAIL (with notes)
...

Overall: PASS / CONDITIONAL PASS / FAIL
```

### 5.7 Bottleneck classification

Each bake-off cell (shape × N × method × GUC) is labeled with its **binding bottleneck**, drawn from {B1 WAL, B2 SPI, B3 LWLock, B5 wake, CPU-other}. The label makes the surface interpretable: a cell at 30K events/sec labeled B2 (SPI) calls for different optimization than the same cell labeled B1 (WAL).

Metric collection per cell:

- **B1 WAL**: capture `pg_stat_wal.wal_bytes` and `wal_fsync_time` deltas across the run. If observed fsync rate ≥ 80% of hardware's measured fsync ceiling, label B1.
- **B2 SPI**: instrument committer with cumulative SPI-call time. If `sum(spi_time) / wall_time` across active committers > 0.5, label B2.
- **B3 LWLock**: `pg_locks_sampler` at 100ms intervals captures wait-event class. If wait time on the shard's tranche dominates total wait time, label B3.
- **B5 wake**: committers timestamp each `SetLatch` call; waiters timestamp wake-to-return. If `p99(wake_to_return)` is the dominant slice of `p99(end_to_end_apply_latency)`, label B5.
- **CPU-other**: residual. If none of the above explains, label CPU-other; expected causes include kernel scheduling overhead, buffer-pool contention, lock manager overhead — document specifics from `perf` profiling if CPU-other is dominant.

The classification is computed automatically by the bake-off harness from the recorded metrics. Manual review of the labels is part of the validation gate: an unexpected bottleneck label (e.g., B3 dominating on a workload where contention shouldn't matter) flags a design problem.

### 5.8 Pass criteria interpretation

The bake-off produces the surface; the validation criteria from §4 interpret it. The bake-off itself doesn't pass or fail; the criteria do, based on the surface.

For "conditional pass": criteria failing must come with a documented mitigation path. Example: P3 (p99 < 50ms at moderate load) misses by 10% → conditional pass if root cause is identified (e.g., B5 wake latency for >500-waiter batches) and a future fix is queued.

---

## 6. PoC implementation milestones

Suggested sequence for implementing the PoC. Each milestone has a verification gate before the next.

**M0: Skeleton (1-2 weeks).** Extension loads, GUCs registered, shmem allocation works, `_PG_init` runs the recovery worker (which is a no-op on first start). Single dummy `#[pg_extern]` returns "hello world."

**M1: Single-shard, single-backend (1-2 weeks).** Queue + slot + committer logic in one shard. One backend can call apply, become committer, drain a batch of 1, write a fake row, return result. No real method.

**M2: Real methods (2-3 weeks).** FIFO, AVG, STD impls behind the trait. Plan_apply produces correct results against pre-seeded data. Includes the minimum SPI to build snapshots. Implements per-event partial-success flow (§3.10) explicitly.

**M3: Multi-backend on single shard (1-2 weeks).** Multiple backends push to one shard; committer election works; one becomes committer, others wait. Verify per-shard correctness. Picks pool-lock mechanism per Q-A.

**M4: Multi-shard (1 week).** Hash dispatch routes to multiple shards. Cross-shard work runs in parallel.

**M5a: Committer death + waiter cancel (1-2 weeks).** Implement orphan recovery (§3.2) including pg_xact-status polling, slot abandonment (§3.3) including dedup-replay on retry. The two most subtle failure modes; verified together because they share infrastructure (slot state machine).

**M5b: Postmaster restart + lease timeout (1-2 weeks).** Recovery worker at _PG_init scans for unrecovered committer transactions; emits compensations for aborted user-tx. Lease-takeover with pg_pid_alive verification. Postmaster-restart test (kill -9, restart, verify).

**M5c: Backpressure + slot exhaustion (1 week).** Push backpressure; queue-full timeout; slot leak audit.

**M6: Compensation semantics (1-2 weeks).** XactCallback ABORT → enqueue compensation request; committer processes compensations; recovery on postmaster restart handles in-flight compensations using user_tx_xid.

**M7: Reservation semantics (1 week, optional for PoC pass).** Alternative apply path under GUC. **Not measured in primary bake-off surface**; primary surface is compensation. M7 is alternative-path-exists demonstration, exercised in correctness tests but not performance tests.

**M8: Observability (1 week).** Metrics functions; per-shard stats; per-method stats. Bottleneck-classification metrics from §5.7.

**M9: Bake-off harness (2 weeks).** Load generators for each workload shape. Statistical runner. Output formatter. Bottleneck classifier. Run the full bake-off.

**M10: Validation report (1 week).** Run criteria against measurements; produce `BENCHMARK_RESULTS.md`; decide pass/fail/conditional.

Total: ~3.5-4.5 months for the M5 split. Adjust per team size.

---

## 7. Open issues (PoC-specific)

Items where the spec doesn't have a definitive answer; resolution requires implementation evidence.

**[Q-A] Pool-lock granularity. RESOLVED (M3.2, acct-4d4n.8, 2026-05-15) — no signal at M3 scope; default `'none'`.** PoC originally sketched a `poc_pool_locks` table for committer-to-committer serialization on the same pool (one extra SPI per batch), with `poc_pool_lock_anchors` as the row-lock-anchor alternative. M3.2 implemented all three modes behind GUC `poc_ledger.pool_lock_mode` and benched single-shard fan_in (8 backends × 15s × 3 runs × 3 modes). Medians: none=747, pool_locks=743, pool_lock_anchors=731 tps — within ~2%, every mode's IQR contains every other mode's median. Structural reason: M3.1's committer-PID CAS election already serializes committer-to-committer within a shard, and the current `pool_hash → shard_idx` routing maps each pool to exactly one shard. Pool-lock is redundant at M3 scope. Default `'none'`; `'pool_locks'` and `'pool_lock_anchors'` retained behind the GUC for re-measurement at M4.1 once multi-shard hash routing makes cross-shard pool collisions reachable. Bench output: `poc/queue-extension/bench/results-m32-pool-lock-fan-in.md`.

**[Q-B] Multi-method batches in one committer-tx.** PoC groups by (pool, method) within a drained batch; each group runs separately but under the same committer sub-tx. Alternative: separate sub-tx per group. Same-sub-tx is simpler but a method-error rolls back work for other methods. Decide during M5 based on whether method-isolation matters.

**[Q-C] Spillover arena allocation policy.** PoC uses a freelist; deallocations return blocks. If fragmentation becomes an issue, a slab allocator may be needed. Measure during M5 / soak test.

**[Q-D] Result-slot recycling under cancel.** When a slot is abandoned, it cycles back to free immediately or waits for the next batch's processing? Faster cycling avoids leaks; lazy cycling means dead-backend detection has more state. Defaulting to immediate cycle with safety audit, but might need adjustment if leaks observed.

**[Q-E] Committer-tx-id allocation.** PoC uses `next_committer_tx_seq` per shard. Alternative: a global PG sequence. Per-shard is faster but recovery scanning is per-shard. Either works; PoC uses per-shard.

**[Q-F] Postmaster-restart recovery for compensations.** §3.5 says "scan for committer_tx_ids whose user-tx is aborted in pg_xact." Cost depends on table size. For PoC: scan is bounded by tables seeded for the test; for production it'd need incremental tracking (last-recon-watermark). Real solution deferred to design-v2.

**[Q-G] Backpressure behavior under sustained overload.** PoC times out at 5s by default. If the queue is sustained-full for minutes, what's the right behavior? Currently: callers time out, abort, retry. Alternative: shed load (return queue_full immediately without waiting). Measure under stress.

**[Q-H] Per-shard lease holder fairness.** Current design: whoever pushes first after a free committer slot wins. Under heavy concurrent push, this could starve some backends consistently. Probably fine for PoC; revisit if observed.

---

## 8. What this PoC explicitly defers to design-v2

Listed once, definitively, so reviewers don't expect them:

- Full trait protocol (4 required + 6 optional methods, associated-type snapshots).
- WAC family methods (perpetual, periodic, retroactive) and close-hook DAG.
- Provisional flagging (`posting_lines_provisional` lifecycle).
- Variance routing (the four patterns: internal-chain, leaf-single-leg, leaf-two-leg-wash, mixed-parent-component).
- Multi-currency.
- Lot/serial identity dimensions.
- Analytical dimensions (routing_op, project, etc.).
- BOM orchestration (multi-item batches per WO).
- Account-isolated qty divisor for WAC (R1 invariant in design-v2; not applicable to PoC because PoC uses simple sku-keyed pools, not class-typed accounts).
- Credit-first dispatch (PoC events have a single SKU; no credit/debit leg distinction).
- Integration with acct's posting_lines, posting_line_inventory, posting_line_sources.
- Replica/HA story.
- Recon job against external posting_lines.
- Archival of fully-reconciled state.
- Trait-vs-registry split (PoC is trait-only for its three methods).

All of these are in design-v2's scope. The PoC validates the foundation those depend on.

---

## Appendix A: Test harness reference

The `poc_test_harness` Rust crate provides:

- **Workload generators**: produces event streams matching the §5.2 shapes.
- **Backend orchestration**: spawns N psql connections, coordinates via barriers, collects per-backend metrics.
- **Fault injection**:
  - `inject_committer_failure(kind)`: triggers tx failure, sigkill, slot abandonment at specific points.
  - `corrupt_shmem(region, pattern)`: best-effort corruption synthesis.
  - `kill_postmaster(immediate=true)`: clean test of restart recovery.
- **Invariant verification**:
  - `assert_invariants(ledger)`: runs all I1, I4, I5, idempotency, compensation checks.
  - `assert_no_drift(snapshot_before, snapshot_after, expected_delta)`.
- **Property-based generation** via `proptest`.
- **Latency histogram** via `hdrhistogram`.
- **`pg_locks_sampler`** integration.

Test files organized:
- `tests/correctness/`: §3 failure-mode tests, §4.1 invariants.
- `tests/concurrency/`: multi-backend stress, scaling tests.
- `tests/performance/`: bake-off shapes (instrumented runs, not the bake-off itself).
- `tests/recovery/`: postmaster-restart, orphan-recovery scenarios.
- `tests/property/`: proptest-based randomized sequences.

---

## Appendix B: Relationship to design-v2

This document validates the foundation. `design-v2.md` describes what gets built on top once the foundation is sound.

**Cross-references from this doc to design-v2:**
- Deferred items in §8 → design-v2 sections.

**Cross-references from design-v2 to this doc:**
- §4 (concurrency architecture) → this doc, §1 and §3.
- §4.7 (bake-off methodology) → this doc, §5.
- §6 (shmem layout) → this doc, §1.4.
- §10 OPEN-* (open issues) → this doc, §7 Q-* (subset; many design-v2 opens require PoC results to close).

**Update cadence:**
- This doc is mostly write-once. After PoC completion, `BENCHMARK_RESULTS.md` is pinned; this doc becomes a historical artifact.
- design-v2 is a living document that evolves with the trait protocol and acct integration.

**Decision authority:**
- PoC pass authorizes design-v2 implementation.
- PoC fail or conditional-pass triggers a design review (the queue architecture may need rework, or specific failure modes need redesign before proceeding).
- PoC results inform GUC defaults, shard count recommendations, and operational guidance in design-v2's operational section.

---

## §M10. Verdict

**Outcome: CONDITIONAL PASS.**

Reported 2026-05-16 against PoC commit `1c61fc1` (post-M9.3 ship). Full evidence: `poc/queue-extension/BENCHMARK_RESULTS.md`.

| criterion | result |
|---|---|
| C1 failure-mode recovery (must-pass) | PASS |
| C2 invariants under property testing (must-pass) | CONDITIONAL — per-milestone chain; consolidated proptest deferred |
| C3 determinism (must-pass) | PASS |
| C4 idempotency under retry (must-pass) | PASS |
| **P1 disjoint scales ≥ 24× N=1 (must-pass)** | **CONDITIONAL** — 14.2× measured; root cause in shard LWLock; shard-count mitigation filed |
| P2 same-pool serializes correctly (must-pass) | PASS |
| P3 p99 < 50ms at 50% peak (must-pass) | PASS — 15.7ms |
| P4 mixed-method within 30% of best (must-pass) | PASS — 98% |
| O1 7-day soak (should-pass) | DEFERRED — followup filed |
| O2 recovery SLAs (should-pass) | PASS |
| O3 observability metrics (should-pass) | PASS |

**Decision:** design-v2 construction is AUTHORIZED subject to the P1 shard-count mitigation being addressed in design-v2's architecture before its own performance milestones.

**Filed followup issues (post-verdict):**
- `acct-7pre` — consolidated proptest harness exercising all seven invariants under random kill/cancel sequences.
- `acct-hubz` — 7-day soak test against the recommended-default GUCs.
- `acct-hjoq` — shard_count GUC sweep at N=32 / N=64 / N=128 to find the smallest configuration that achieves the 24× P1 bar.

**Headline numbers (from BENCHMARK_RESULTS.md):**
- Peak durable (sc=on): 6379 evps fan_out N=128 · 8017 evps small_batch N=128 · 11878 evps fan_in N=256
- Peak relaxed (sc=off): 6694 evps fan_out N=128 · 8418 evps small_batch N=128
- Recommended design-v2 GUC defaults: `batch_window_us=500 batch_size_max=1024 synchronous_commit=on`
- Bottleneck transitions cleanly: IO:WalSync (N=1–2) → LWLock:WALWrite (N=4–8) → Extension:Extension (N=16+, the P1 mitigation target)
- 781 runs across 161 cells: **0 errors, 0 deadlocks** end-to-end.
