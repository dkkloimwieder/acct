> **ARCHIVED — SUPERSEDED (2026-08-07).** Rescued from `poc/design_research/design-v2.1.md`,
> where it existed only as an uncommitted working copy. The v2.1 architecture (shmem
> staging queue, router, committer pool, document-level lexicographical locking) was
> retired by the `acct-0at4.11.5` gate verdict — "machinery not justified"; the surviving
> architecture is staging-table + single-statement + alt-C + logical-decoding feed
> (ledger-v3.2 line). Archived per convergence decision Q5 (2026-08-07).
>
> Kept verbatim because `acct-mpjz` catalogs the §14 alternatives ("Alternatives Flagged
> for In-Tandem Testing") — section numbering below must stay stable. Historical record;
> do not edit.

# Unified Costing Ledger Extension — Design Specification v2.1

**Status:** Target Architecture Specification (standalone alternative to design-v2.md)
**Target:** PostgreSQL 18+, pgrx 0.17+
**Audience:** Implementation team and future maintainers
**Relationship to design-v2:** This is an alternative architecture, not a replacement. design-v2.md remains valid as the per-item-shard-and-streaming-commits target. v2.1 targets document-level transactional atomicity via lexicographical locking and bulk array-unnest writes. The PoC at /mnt/user-data/outputs/poc-validation-spec.md validates the queue+committer primitives shared by both architectures.

**Inherited from design-v2 without modification:**
- The trait protocol (4 required + 6 optional methods, associated-type snapshots).
- R1–R7 invariants from acct's CLAUDE.md.
- The close-hook DAG (§5.4 of design-v2): pool iteration in Kahn order, merged value/qty replay.
- Cost-method registry vs. trait split (FIFO/specific-id/standard in trait; WAC family in plpgsql cost_method_strategies registry).
- Dimension vocabulary (identity: Lot, Unit, CostLayer, CostBook; analytical: routing_op, project, EAV-extensible).

This document focuses on what v2.1 changes: the **concurrency execution model** (schema lock domains, queue, router, committer pipeline) and its consequences for failure handling, recovery, and the application-tier contract.

---

## 0. Document Map

- §1: Schema contract — tables this extension owns, tables it writes (acct's), tables it reads.
- §2: Lock domains — what FOR UPDATE acquires, how lexicographical ordering works across SKU-keyed and WO-keyed pools.
- §3: Invariants — R1–R7 plus v2.1-specific invariants (idempotency, sole-writer, drain-to-zero).
- §4: The Universal Transaction Envelope — what callers submit, the strict R/W set contract, the synchronous-enqueue boundary.
- §5: Shmem layout — staging queue, committer queue, slot states, transitions.
- §6: The Router — affinity-grouping algorithm, fairness rule, death/recovery.
- §7: Committer pipeline — the 5-step execution: lex-lock, dedup, hydrate, dispatch, bulk insert.
- §8: Trait dispatch — how event_type routes to the right cost method, how v2.1 reuses design-v2's trait protocol.
- §9: Failure & recovery — committer death, partial-batch failure, caller user-tx rollback, postmaster restart.
- §10: Close hook integration — drain-to-zero, period boundary semantics.
- §11: Currency model (single-currency-per-transaction), WAC running-average state, dimensions, lots, units, BOM orchestration, cost books.
- §12: Webhooks and status observation — DB-side API for terminal-state delivery.
- §13: GUCs, monitoring, operational concerns.
- §14: Alternatives flagged for in-tandem testing.
- §15: Open issues.

---

## 1. Schema Contract

### 1.1 Tables this extension owns

These tables are created and exclusively written by the extension. The "sole-writer invariant" (§3.6) is load-bearing for several mechanisms (sequence hydration, dedup-lookup, idempotency) and is enforced operationally — application code does not write directly to these tables.

**`pool_locks`** — the lock domain for SKU-keyed pools (raw, finished-goods).

```sql
CREATE TABLE pool_locks (
    sku_id       BIGINT NOT NULL,
    location_id  BIGINT NOT NULL,
    lock_version BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (sku_id, location_id)
);
```

The row exists solely as a target for `SELECT ... FOR UPDATE`. The `lock_version` column is a no-op self-update target if a committer wants to mark "this pool was touched" without inserting elsewhere. A row is inserted lazily on first access to a pool; deletion is never done in normal operation.

Currency is intentionally absent from pool identity; see §11.1 for the single-currency-per-transaction model.

**`wip_pool_locks`** — the lock domain for WO-keyed WIP pools.

```sql
CREATE TABLE wip_pool_locks (
    work_order_id BIGINT NOT NULL,
    operation_id  BIGINT NOT NULL,
    lock_version  BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (work_order_id, operation_id)
);
```

Separate from `pool_locks` because WIP pools have no SKU and their identity is `(work_order_id, operation_id)`. See §2 for why these are separate domains rather than NULL-handled.

**`ledger_submission_status`** — terminal-state observation table. See §12.

```sql
CREATE TABLE ledger_submission_status (
    correlation_id UUID PRIMARY KEY,
    state          TEXT NOT NULL CHECK (state IN ('queued', 'processing', 'committed', 'failed', 'replayed')),
    business_date  DATE NOT NULL,
    enqueued_at    TIMESTAMPTZ NOT NULL,
    processed_at   TIMESTAMPTZ,
    committed_at   TIMESTAMPTZ,
    error_code     TEXT,
    error_detail   JSONB,
    committer_tx_id BIGINT,
    superbatch_id  BIGINT
);
CREATE INDEX ledger_submission_status_enqueued_at ON ledger_submission_status (enqueued_at);
CREATE INDEX ledger_submission_status_state ON ledger_submission_status (state) WHERE state IN ('queued', 'processing');
```

The `business_date` column is populated at row creation (by enqueue under `caller_intx` mode, or by the committer's lazy fallback) from the envelope's payload. It is the business effective date the application assigned to the envelope. This column is essential for the close-hook drain-to-zero check (§10.1), which must determine whether any in-flight envelopes target the closing period without reading shmem-only payloads.

There is intentionally **no separate `business_date` partial index**. A second partial index sharing the predicate `WHERE state IN ('queued', 'processing')` would double the index-churn cost on every router and committer state transition (every queued → processing → committed/failed move adds and removes rows from the partial-index scope). Since the in-flight set is bounded (a few thousand rows at most under normal operation; backpressure caps it), the drain-to-zero check is fast enough using only the `state` partial index: bitmap-scan all in-flight rows (small) and filter by `business_date` in memory. See §10.1 for the drain query.

The state transitions are `queued → processing → (committed | failed | replayed)`. State is updated by the router (queued → processing on routing assembly start; processing → queued on router crash sweep) and by the committer (processing → committed/failed/replayed on top-level transaction commit).

The partial index on the pending states keeps the in-flight set quickly queryable for operational dashboards without scanning the historical bulk.

**Webhook queue** — a separate persistent table for at-least-once webhook delivery. See §12.

```sql
CREATE TABLE webhook_deliveries (
    delivery_id     BIGSERIAL PRIMARY KEY,
    correlation_id  UUID NOT NULL REFERENCES ledger_submission_status(correlation_id),
    payload         JSONB NOT NULL,
    target_url      TEXT NOT NULL,
    state           TEXT NOT NULL CHECK (state IN ('pending', 'in_flight', 'delivered', 'permanent_failure')),
    attempt_count   INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX webhook_deliveries_next_attempt ON webhook_deliveries (next_attempt_at) WHERE state IN ('pending', 'in_flight');
```

**`wac_pool_state`** — incrementally-maintained running average for WAC perpetual / AVG methods. See §8.3 for the access pattern.

```sql
CREATE TABLE wac_pool_state (
    sku_id               BIGINT NOT NULL,
    location_id          BIGINT NOT NULL,
    avg_unit_cost        BIGINT NOT NULL,
    avg_total_qty        BIGINT NOT NULL,      -- signed pool depth
    last_updated_at      TIMESTAMPTZ NOT NULL,
    last_committer_tx_id BIGINT NOT NULL,
    PRIMARY KEY (sku_id, location_id)
);
```

The row is created and updated by a single Step 5 UPSERT (`INSERT ... ON CONFLICT (sku_id, location_id) DO UPDATE`) — see §7.7 and §8.3. The same statement creates the row on a pool's first receipt event and refreshes the running average on subsequent events. There is no separate creation step; the `pool_locks` FOR UPDATE held by the committer makes the UPSERT race-free against concurrent committers. For lot-tracked SKUs, a corresponding `wac_pool_state_lot` table keyed by `(sku_id, location_id, lot_id)` exists with the same UPSERT semantics.

This table is the source of truth for the current average. The close-hook's variance corrections (per design-v2 §5.4) must acquire the same `pool_locks` FOR UPDATE and update this table alongside `posting_lines_provisional` finalization to keep the running average consistent with retroactive corrections.

Other WAC variants (periodic, retroactive) have analogous state tables tailored to their semantics; the names and schemas are introduced by their respective method implementations.

**`ledger_persistent_staging`** — durable backup of envelopes submitted with `durable_queue=true` (§4.7). Exists only when the deployment has `ledger.persistent_staging = on`. When disabled, the table is not created and `durable_queue=true` requests are rejected at the function level.

```sql
CREATE TABLE ledger_persistent_staging (
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
CREATE INDEX ledger_persistent_staging_state
  ON ledger_persistent_staging (state, enqueued_at)
  WHERE state IN ('staged', 'in_shmem');
```

The `state` field tracks the row's lifecycle:
- `staged` — INSERTed by the caller's enqueue under `durable_queue=true`; the caller's user-tx may or may not have committed yet.
- `in_shmem` — committer has confirmed the envelope is in the shmem staging queue (post caller-tx commit verification).
- `completed` — committer committed the cost rows; this persistent row may be GC'd after `ledger.persistent_staging_gc_retention_hours` (default 24).

This table is the durable backup. Recovery on postmaster restart (§9.4) replays uncompleted rows back into the shmem staging queue. A separate GC worker periodically deletes `completed` rows past the retention horizon.

### 1.2 Tables this extension writes (acct's contract)

The extension writes to acct's existing cost tables per the acct schema contract. This is direct schema integration, not a separate write target.

**Inheriting from acct's migrations 0019–0024 (per design-v2 §1.2 and the prior corrections):**

- `posting_lines` — the canonical append-only event log. Every cost-affecting event produces one or more posting_lines. Carries the envelope's pinned currency (the subsidiary's base currency at the time of submission) directly on the posting line, not as a separate expansion table.
- `posting_line_inventory` — qty-side detail attached to a posting_line (sku_id, location_id, qty, layer attribution).
- `posting_line_sources` — provenance (document_id, document_type, business_date, doc_chrono).
- `posting_line_dimensions` — EAV-style analytical dimensions (routing_op, project, etc.).
- `posting_lines_provisional` — lifecycle marker for WAC-class methods. See §10 for close-hook interaction.
- `cost_layers` — FIFO/LIFO/specific-id layer pool. Each layer is one row; born_at / born_seq monotone per pool.
- `cost_layer_depletions` — layer-attributed consumption rows. One row per (consumption_event, layer_touched).
- `cost_consumptions` — consumption rows for methods that don't track per-row layer state (WAC, standard, AVG).
- `inventory_lots`, `inventory_units` — lot- and serial-level identity dimensions when in scope.

**v2.1-specific columns to add to the cost tables (extending design-v2's existing contract):**

Each of `cost_layers`, `cost_layer_depletions`, `cost_consumptions` gains:

```sql
correlation_id  UUID NOT NULL,
user_tx_xid     xid8 NOT NULL,
committer_tx_id BIGINT NOT NULL,
superbatch_id   BIGINT NOT NULL
```

`correlation_id` links every cost row back to the originating caller submission (one envelope → one correlation_id → potentially many cost rows). `user_tx_xid` is the caller's transaction XID (for compensation-style recovery if ever needed, and for posting-line attribution). `committer_tx_id` and `superbatch_id` link to the execution context that wrote the row (for operational diagnostics).

**Idempotency constraints:**

```sql
ALTER TABLE cost_layer_depletions ADD CONSTRAINT
  cost_layer_depletions_idempotent UNIQUE (issue_id, method_used, layer_id);

ALTER TABLE cost_consumptions ADD CONSTRAINT
  cost_consumptions_idempotent UNIQUE (issue_id, method_used);
```

The depletions constraint must allow multiple rows for the same `(issue_id, method_used)` because a FIFO consumption can span multiple layers. The `layer_id` discriminator makes each row unique within the issue. The consumptions constraint is the simpler form because AVG/STD/WAC produce one consumption row per issue.

**Flagged concern (§14 A1):** the depletion constraint as written does NOT catch caller-bug issue_id-reuse across different `(sku, location)` pools (different layer_ids in each pool, so each row passes the constraint). The dedup-lookup in §7 also misses this case for depletions. Caller contract is "issue_ids globally unique"; the schema is partial enforcement. See §14 for alternative: encode `(sku_id, location_id)` into the constraint via the layer's pool reference.

### 1.3 Tables this extension reads

- `accounts` — for resolving an account's `kind` (class) when constructing pool keys. R1 isolation depends on account-id-typed filters.
- `sku_method_assignments` (or equivalent acct configuration) — determines which cost method a SKU uses.
- `work_orders`, `operations` — for resolving WIP pool identity and for sequencing the close-hook DAG.
- `accounting_periods` — for close-hook timing.

Currency-related tables (FX rate tables, currency master) are not consulted by the extension. The envelope's currency is pinned by the caller at submission time; the extension treats currency as opaque metadata on posting_lines, not as an axis for cost computation.

### 1.4 Schema migration story

For acct adoption:

1. Add `correlation_id`, `user_tx_xid`, `committer_tx_id`, `superbatch_id` columns to existing cost tables. NOT NULL with a default expression cannot be enforced retroactively; the migration creates them nullable, backfills historical rows with sentinel values (e.g., `correlation_id = uuid_nil()`, `user_tx_xid = '0'::xid8`, etc.), then sets NOT NULL.
2. Add the UNIQUE constraints. Pre-validation pass required: historical data must already satisfy these. If acct's historical issue_ids have legitimate reuse patterns the new constraints would reject, the migration stalls and the issue_ids must be rectified first. Worth a discovery query before migration: `SELECT issue_id, method_used, COUNT(*) FROM cost_consumptions GROUP BY 1,2 HAVING COUNT(*) > 1;` must return zero rows.
3. Create `pool_locks`, `wip_pool_locks`, `ledger_submission_status`, `webhook_deliveries`, `wac_pool_state` (and any variant tables like `wac_pool_state_lot` for lot-tracked SKUs), and `ledger_persistent_staging` (only if `ledger.persistent_staging=on` is being enabled).
4. Populate `pool_locks` and `wip_pool_locks` from existing pool inventory (a single backfill query). Going forward, these are lazily inserted on first access by the committer.
5. Populate `wac_pool_state` from acct's existing cost history: for every (sku, location) pair using a WAC-class method, compute the current running average from `posting_line_inventory` history and insert one row. This is a one-time backfill; going forward the state is incrementally maintained by Step 5 UPSERTs. Verify post-backfill that recomputing the average from history matches the seeded value to within rounding tolerance.
6. Add `business_date DATE` column to `ledger_submission_status` if migrating from a prior version of this design; new deployments include it from the CREATE TABLE.
7. **`posting_line_currencies` transition.** Under v2.1's single-currency model (§11.1), the extension does NOT write to `posting_line_currencies` for new envelopes. Two practical options for the transition period:
   - **Reporting-compatible mode:** for each posting_line the extension writes, also insert one row into `posting_line_currencies` with the pinned currency and the posting line's amount. Keeps legacy reports working. Mild write overhead. Recommended for organizations with significant reporting infrastructure that joins on `posting_line_currencies`.
   - **Cutover mode:** the extension ignores `posting_line_currencies` entirely. Existing reports that join on it must be updated to read currency from `posting_lines` directly. Cleaner long-term; requires coordinated reporting migration.
   Pick per organizational constraints; both are supported via a GUC `ledger.write_posting_line_currencies_compat` (default off / cutover mode for new deployments, on for existing acct migration).
8. Roll out the extension. Existing code paths writing to cost tables must be cut over to the extension's API; until cutover is complete, the sole-writer invariant (§3.6) is violated and v2.1's sequence-hydration mechanism is unsafe. Cutover is therefore atomic-per-deployment, not gradual.

**Flagged concern (§14 A2):** atomic cutover may not be operationally feasible. Alternative: tolerate concurrent legacy writers during transition by replacing sequence hydration with a stronger lock (e.g., hold `FOR UPDATE` on `pool_locks` plus advisory lock during the MAX read and the bulk insert). Slower but transition-safe.

---

## 2. Lock Domains

### 2.1 Two domains, fixed acquisition order

v2.1 has two lock domains: `pool_locks` (SKU-keyed) and `wip_pool_locks` (WO-keyed). They are intentionally separate tables because:

- **WIP pools have no SKU.** A unified table with nullable `sku_id` requires NULL-handling in ORDER BY, NULL handling in PRIMARY KEY (PG allows multiple NULLs by default — bad here), and an additional `kind` discriminator column. Workable but uglier than separate tables.
- **Different identity tuples.** SKU pools are `(sku_id, location_id)`. WIP pools are `(work_order_id, operation_id)`. Distinct tables make these PRIMARY KEYs natural.
- **Different access patterns.** SKU pools are accessed by virtually every event type. WIP pools are accessed only by WO-related events (issue, completion, op-move, scrap). Separation simplifies query planning.

The committer's lock acquisition (§7 Step 2) is therefore TWO queries, not one — first the SKU domain, then the WIP domain. Acquisition order across the two domains is fixed: **SKU domain first, WIP domain second.** This is a hard rule for deadlock-freedom: every committer must acquire in the same domain order.

Within each domain, lex-sort by the domain's natural key:

- SKU domain: ORDER BY (sku_id, location_id).
- WIP domain: ORDER BY (work_order_id, operation_id).

### 2.2 Lock acquisition queries

Both lock-row creation and lock acquisition use **singleton SPI calls in sorted lex order**. Bulk UNNEST forms are NOT used here because PostgreSQL does not guarantee row-acquisition order across `UNNEST + ON CONFLICT` or `UNNEST + ORDER BY + FOR UPDATE` against concurrent writers — the executor may write or lock rows in any order during a scan, recreating the inter-committer deadlock pathology v2.1 is built to avoid. Singleton SPI calls cannot be reordered.

```sql
-- Step 2a: SKU-keyed pools (singleton loop in Rust, in sorted lex order)
SELECT 1 FROM pool_locks
WHERE sku_id = $1 AND location_id = $2
FOR UPDATE;

-- Step 2b: WIP pools (singleton loop, only if the batch touches WIP)
SELECT 1 FROM wip_pool_locks
WHERE work_order_id = $1 AND operation_id = $2
FOR UPDATE;
```

If a pool row doesn't exist (first-time access), the lock query returns no row for that key; the committer falls through to lazy creation in the same singleton loop:

```sql
INSERT INTO pool_locks (sku_id, location_id) VALUES ($1, $2)
ON CONFLICT DO NOTHING;
```

Then re-run the locking SELECT for that pool. The ON CONFLICT DO NOTHING handles the race where two committers race to create the same lock row; only one wins and both proceed to the locking SELECT which now finds the row.

The four queries (two INSERT-ON-CONFLICT + two FOR UPDATE) MUST be prepared once per committer and reused; non-prepared SPI per-call cost (~50μs) would dominate the lock-acquisition budget. Prepared per-call cost is ~5-10μs.

**Flagged concern (§14 A3):** eager creation at SKU/WO setup time moves the lazy-INSERT cost off the hot path entirely: the pool_locks and wip_pool_locks rows exist before any committer needs them; Step 2 becomes just the FOR UPDATE singleton loop. Per-SuperBatch SPI cost drops from ~2-4ms to ~0.5-1ms in lock-heavy workloads. Production deployments can run eager creation and treat lazy as the fallback safety net. Tradeoff is operational: eager creation requires a setup step (creating rows when SKUs/WOs are first registered, or via an explicit `ledger_ensure_pool_locks_exist(...)` function called during application provisioning). PoC measures both modes.

### 2.3 What's NOT locked

Notably, the committer does NOT acquire FOR UPDATE on `cost_layers`, `cost_layer_depletions`, `cost_consumptions`, `posting_lines`, or any acct table. The `pool_locks` row is the rendezvous; everything else relies on the sole-writer invariant (§3.6).

The benefit: write paths are append-only and don't compete with read paths (close hooks, reporting queries) on row locks. The cost: any code path that writes to these tables outside the extension is a sole-writer-invariant violation and breaks v2.1.

### 2.4 SuperBatch composition

A SuperBatch is a group of envelopes the router has determined must be processed together because they share state. One SuperBatch is processed by one committer in one top-level transaction.

**The affinity rule.** Two envelopes are in the same SuperBatch if they share at least one pool_key, OR if there is a chain of envelopes between them where each adjacent pair shares a pool_key. In graph terms: each SuperBatch is one connected component of the overlap graph over the router's window.

The router implements this with union-find: for each envelope in the window, union it with every other envelope that touches any of the same pool_keys. Each connected component becomes one SuperBatch.

**Why active grouping rather than FIFO or disjointness.**

Two earlier framings of this rule were wrong:

- **Disjointness-mandated** (the original v2.1 draft): the router refused to put overlapping envelopes in the same SuperBatch. Overlapping envelopes were fanned out to separate SuperBatches and processed by separate committers, which then serialized via FOR UPDATE on the shared pool_locks rows. This forced inter-committer blocking on every overlap — the exact pathology v2.1 was built to avoid.

- **FIFO packing** (a subsequent draft): the router took envelopes from the window in arrival order, ignoring pool_key overlap entirely. This was correct when overlapping envelopes happened to arrive consecutively but wrong under concurrent submission from many backends, where overlapping envelopes interleave with non-overlapping ones in the queue. Example: queue is `[Env_A on SKU 5, Env_B on SKU 6, Env_C on SKU 5]` with `batch_size_max=2`. FIFO produces SB-1 = {Env_A, Env_B} and SB-2 = {Env_C}. Env_A and Env_C are now in different SuperBatches; their committers contend on SKU 5 via FOR UPDATE. The exact pathology the disjointness fix was supposed to eliminate.

Active affinity grouping fixes both. Env_A and Env_C are routed to the same SuperBatch because they share SKU 5. Env_B (independent) goes to a separate SuperBatch that runs in parallel. One committer handles the SKU-5 contention sequentially via in-memory snapshot mutation; another committer processes Env_B in parallel. No inter-committer FOR UPDATE blocking.

**Why overlap within a SuperBatch is correct.**

- The committer's Step 2 acquires FOR UPDATE on the **deduplicated union** of all envelopes' pool_keys. Overlapping envelopes contribute the same pool_key once.
- The committer's Step 3 hydrates each pool's snapshot once. The snapshot is shared across all envelopes' events that touch that pool.
- The committer's Step 4 (`plan_apply`) processes events in chronological order. Each event mutates the in-memory snapshot; subsequent events on the same pool run against the cumulative state. Two events consuming from SKU X see X's qty decrement progressively.
- The committer's Step 5 (bulk UNNEST) writes rows attributed by `correlation_id`.
- The committer's Step 2.5 (dedup-lookup) operates on `(correlation_id, issue_id)` pairs and is scoped to the SuperBatch's rows.

**The split fallback.** If a single connected component contains more than `batch_size_max` envelopes, the component is split into chunks of size `batch_size_max`. The chunks become separate SuperBatches. Cross-SuperBatch contention via PG row locks on `pool_locks` serializes them correctly — this is the genuine fallback for oversized contention groups (workloads with persistent hot pools driving sustained high overlap). Under reasonable defaults (batch_size_max=50), splitting only occurs for genuinely-saturating contention.

**Within-SuperBatch ordering.** Envelopes within a SuperBatch are ordered by `request_seq` (submission order). Events within the SuperBatch are then chronologically sorted (per §7.6) for plan_apply. This preserves causal ordering for envelopes from the same caller that contribute events to shared pools.

The invariant the router maintains is **affinity grouping**: I-affinity-grouping (§3.2). The router's job is to identify the connected components of overlap and route each as a unit to one committer.

---

## 3. Invariants

### 3.1 Carried over from acct (R1–R7)

Defined in acct's CLAUDE.md, enforced here:

- **R1**: account-isolated qty divisor. Snapshot construction reads `SUM(qty SIGNED) WHERE account_id = pool.account_id`; never aggregates across sibling accounts (different SKUs at the same location, WIP accounts attached to WOs at the same location). See design-v2 Appendix A.7 for the full reasoning.
- **R2**: credit-first SKU resolution. For events with both debit and credit legs, the credit-side SKU determines the cost method and the pool identity.
- **R3**: append-only with named exceptions. `posting_lines` is append-only via trigger. `posting_lines_provisional`'s `finalized_at`, `variance_amount`, `variance_posting_line_id` columns are write-once at close.
- **R4**: lock-before-snapshot. The pool_locks row's FOR UPDATE precedes any read of pool state for cost computation. In v2.1 this happens at the committer (Step 2 → Step 3), not the caller. The R4 semantic shift: in synchronous in-tx models the caller holds the lock; in v2.1 the committer holds it. Reviewers porting in-tx patterns will look for the lock in the caller's path and not find it; this is by design.
- **R5**: monotone per-pool sequences. `consumed_seq` monotone within `(layer_id, consumed_at)`; `born_seq` monotone within `(sku_id, location_id, born_at)`.
- **R6**: posting-line provisional flagging for WAC-class methods at apply-time, post-credit-first-resolution.
- **R7**: document-level unit_cost snapshot sourced from post-lock dispatcher output. In v2.1, the dispatcher output is the bulk-UNNEST insert; the document-level unit_cost (if needed, e.g., on a shipment line) is captured from the committer's in-memory result before INSERT and delivered via webhook to the application tier. The application tier writes it back to the document. This is a meaningful change from synchronous models where R7 is enforced inline.

**Flagged concern (§14 A4):** R7 in v2.1 requires the application tier to honor the contract — wait for the webhook before treating its document as fully posted. Alternative: write the document-level unit_cost from the committer into a dedicated `document_unit_cost_results` table; the application tier reads it instead of relying on webhooks. Stronger durability, more schema, more SPI in the committer's hot path.

### 3.2 v2.1-specific invariants

**I-idempotent-replay** (v2.1-1): a caller submitting the same envelope twice (same `(issue_id, method_used)` set in the payload) produces row-identical results from the database's perspective. The second submission's dedup-lookup finds the first's rows and skips re-execution. The terminal state delivered to the caller for the replay is `replayed` (distinct from `committed`), allowing the application tier to know it was a no-op replay if needed.

**I-affinity-grouping** (v2.1-2): for every pair of envelopes (A, B) that share at least one pool_key AND both are pending in the router's window at the same tick, A and B are routed into the SAME SuperBatch, OR into chunks of the same connected component if the component exceeds `batch_size_max`. The router actively groups envelopes by transitive pool_key overlap via union-find (§6.2). Verified by `test_affinity_groups_overlapping_envelopes`: inject interleaved overlapping envelopes (e.g., [SKU 5, SKU 6, SKU 5] with batch_size_max=2), assert SKU-5 envelopes land in the same SuperBatch.

**I-pool-snapshot-consistency** (v2.1-2a): within a SuperBatch, events that touch the same pool are applied in chronological order (per §7 Step 4 sort) against a shared per-pool in-memory snapshot. After event N, the snapshot reflects the cumulative effect of events 1..N; event N+1 on the same pool sees the post-event-N state. The committer's sequential plan_apply with snapshot mutation handles within-SuperBatch overlap correctly; the affinity grouping in I-affinity-grouping ensures all overlapping work is colocated to be handled this way.

  **Path-dependent failure handling.** For path-dependent costing methods (FIFO, AVG, WAC variants), a single envelope's failure cannot be resolved by in-place "delta rollback" — subsequent envelopes' events that already booked rows against the to-be-reverted intermediate state would be corrupted. The committer therefore uses **pristine-snapshot replay**: a clone of the post-Step-3 snapshot is held immutable; plan_apply runs against a working clone; on any envelope failure the working clone is discarded and a new pass starts from pristine with the failed envelope excluded. See §7.6 for the algorithm and a worked example.

**I-causal-snapshot-observability** (v2.1-2b): when committer A's top-level transaction commits while committer B's transaction is blocked on FOR UPDATE for the same pool, committer B's subsequent snapshot hydration SELECT observes committer A's committed rows. This is load-bearing for causal-chain workloads (PoReceipt → WoComplete consuming the receipt → SoShipment of WO output): under READ COMMITTED isolation (per §7.3), each statement gets a fresh snapshot, and FOR UPDATE blocking semantics ensure the snapshot is taken AFTER the holder's commit. Stricter isolation levels (REPEATABLE READ, SERIALIZABLE) freeze the snapshot at transaction start and break this invariant. The implementation MUST run committer transactions at READ COMMITTED.

**I-upsert-array-unique** (v2.1-2c): every bulk UPSERT input array passed to a Step 5 `INSERT ... ON CONFLICT DO UPDATE` statement contains exactly one entry per conflict-target key. Specifically: the `wac_pool_state` UPSERT input array contains one row per (sku_id, location_id) — the FINAL cumulative state after all events in the SuperBatch have applied; the `ledger_persistent_staging` completed-transition UPDATE matches one row per correlation_id; the status UPSERTs (§7.7) match one row per correlation_id. The committer's array-builder MUST deduplicate by conflict-target key before invoking the bulk UPSERT, otherwise PG raises `ERROR: ON CONFLICT DO UPDATE command cannot affect row a second time` and aborts the SuperBatch's entire transaction.

**I-sole-writer** (v2.1-3): the extension is the only writer to `cost_layers`, `cost_layer_depletions`, `cost_consumptions`, and to the v2.1-owned tables (`pool_locks`, `wip_pool_locks`, `ledger_submission_status`, `webhook_deliveries`, `wac_pool_state`, `ledger_persistent_staging`). Any other writer breaks sequence hydration (§7 Step 3), dedup-lookup (§7 Step 2.5), incremental WAC state (§8.3), durable-staging recovery (§4.7), and lock semantics (§2.3). Enforced operationally and via PG's REVOKE on write privileges to non-extension roles.

**I-drain-to-zero** (v2.1-4): a period's close hook fires only after the staging queue and committer queue are both drained of envelopes targeting that period's date range. See §10.

**I-eventual-resolution** (v2.1-5): every envelope eventually reaches a terminal state in `ledger_submission_status` within `MAX_RESOLUTION_BOUND = 10 × committer_lease_ms` of any failure event affecting it. Inherited from PoC spec; enforced by orphan recovery and the recovery sweep.

**I-router-progress** (v2.1-6): no envelope sits in `pending` state longer than `batch_window_us × 3` under any non-saturated workload. With the affinity-grouping router's oldest-first group dispatch (§6.2), the staging-queue head's connected component is always claimed within one tick. See §6.3 fairness rule for the defensive backstop covering future multi-router designs.

### 3.3 Sole-writer invariant — alternative tested in tandem

**Flagged for tandem testing (§14 A5):** the sole-writer invariant is a strong operational requirement. Acct has many existing code paths writing to cost tables. Three options to validate:

- **(A) Hard sole-writer.** All writes to cost tables go through the extension. Other code paths are refactored or removed. Cleanest design; biggest cutover risk.
- **(B) Soft sole-writer with advisory locking.** Other writers exist but acquire a PG advisory lock per-pool before writing, mirroring the extension's pool_locks FOR UPDATE. Sequence hydration becomes safer but dedup-lookup is still only authoritative for extension-written rows.
- **(C) Multi-writer with stronger committer locks.** Extension's committer acquires explicit FOR UPDATE on the rows it's about to read for hydration (`cost_layers` etc.), not just on `pool_locks`. Slower (more lock contention) but tolerates concurrent writers without invariant violation.

The PoC tested (A). Production deployment may require (B) or (C) depending on acct's cutover constraints. Tandem testing: implement (A) with stub mode toggles for (B) and (C), measure overhead delta on representative workload.

---

## 4. The Universal Transaction Envelope

### 4.1 What callers submit

The extension exposes one SQL-level entry point for all cost-affecting business events:

```sql
SELECT ledger_enqueue(
    correlation_id := $1,         -- UUID, required
    event_type     := $2,         -- text discriminator (see §4.3)
    payload        := $3,         -- JSONB business payload
    pool_keys      := $4,         -- JSONB array of (sku_id, location_id) | (wo_id, op_id)
    durable_queue  := $5          -- boolean, default false (see §4.7 for durability semantics)
);
```

The envelope's currency is pinned by the caller at submission time (the subsidiary's base currency) and stored as a single field on the envelope, NOT as part of the pool_keys. Cross-currency adjustments — converting transaction amounts to the subsidiary's base currency — happen upstream in acct's FX layer BEFORE the envelope reaches the ledger extension. The extension treats currency as an opaque tag carried through to posting_lines; it does not maintain separate pools per currency, does not perform FX conversion, and does not handle multi-currency cost computation.

The `durable_queue` parameter controls whether the envelope survives a postmaster crash that occurs after enqueue success but before the committer processes the cost rows. See §4.7 for the full semantics. Default `false` (shmem-only staging; cheapest path; in-flight envelopes lost on postmaster restart). When `true`, requires the deployment to have `ledger.persistent_staging = on`; otherwise the function raises ERRCODE_FEATURE_NOT_SUPPORTED.

Return: void on successful enqueue. Raises ERRCODE_INSUFFICIENT_RESOURCES on backpressure failure (see §4.5). Raises ERRCODE_FEATURE_NOT_SUPPORTED if `durable_queue=true` is requested without persistent staging available — the function NEVER silently downgrades durability.

The function attempts to push the envelope to the staging shmem queue (and, when `durable_queue=true`, also writes to the persistent staging table). On success, the caller's user-tx may commit or rollback freely; see §4.4 for the user-tx coupling.

### 4.2 The strict caller R/W set contract

The `pool_keys` argument is the **complete topological R/W set** for this envelope. The caller is responsible for:

- BOM expansion: if the envelope is a WO completion that consumes 15 component SKUs and produces 1 finished-good SKU, all 16 pool_keys must be in the array.
- Multi-leg expansion: a transfer envelope touches both source and destination locations; both pool_keys present.

The committer does NOT speculatively expand the R/W set mid-execution. Doing so would break the lex-locking guarantee (would need to acquire additional locks mid-transaction in unsorted order, deadlock risk).

If the caller submits an incomplete R/W set, the committer detects the gap during Step 4 dispatch (the method's `plan_apply` tries to read a pool that wasn't locked) and fails the envelope with `error_code = 'incomplete_rw_set'`. The transaction does not corrupt state — Step 4 is in-memory only.

**Flagged concern (§14 A6):** caller-side BOM expansion duplicates logic that already exists in acct's BOM tables. Two options for the duplicated work:

- **(A) Caller queries acct's BOM tables and expands.** Pure caller responsibility. The extension is BOM-agnostic.
- **(B) Extension exposes a `ledger_expand_pool_keys(event_type, payload)` helper function.** Caller calls this, then passes the result to `ledger_enqueue`. Reduces duplication but means the helper executes BOM expansion logic that lives in plpgsql or extension code rather than in the caller's domain.

Both are workable. (A) keeps the extension narrow. (B) factors out common logic at the cost of expanding the extension's surface. Test (A) first; add (B) if duplication becomes painful.

**Notes on correlation_id, retries, and visibility latency (descriptive, not contract-mandating):**

*Correlation_id idempotency.* Dedup-lookup operates on `(correlation_id, issue_id)` where `issue_id` is derived from event content. Callers using correlation_id as an idempotency token should ensure same-correlation_id implies same-content; submitting different content under the same correlation_id may produce multiple cost-row sets because dedup-lookup will not match. Callers can guarantee content stability by deriving correlation_id from a canonical hash of the envelope payload (e.g., UUIDv5 over a canonical serialization). This is a caller-side practice, not enforced by the extension.

*Unique violation on retry.* `ledger_enqueue` raises `unique_violation` when the correlation_id is already present in `ledger_submission_status`. Caller retry behavior on this signal is application-specific: callers using correlation_id as an idempotency token typically treat it as "envelope already submitted; poll status for outcome"; callers using fresh correlation_ids per attempt should not encounter the situation. The extension does not prescribe a retry policy.

*Failure visibility latency.* Failed envelopes appear in `ledger_submission_status` with bounded latency, but not instantaneously. The committer's lazy fallback fires on its next encounter with the envelope after the caller's user-tx transitions to aborted in `pg_xact`. For polling clients, expect tens-of-milliseconds latency under typical load; up to `caller_tx_timeout_ms` (default 30s) under pathological caller stalls. See §4.4.1 "Failure visibility under caller abort" for the full mechanism.

*Operational guidance for `caller_tx_timeout_ms`.* Set `ledger.caller_tx_timeout_ms <= deployment's statement_timeout`. The committer's eject-bound triggers at `caller_tx_timeout_ms`; PG's `statement_timeout` triggers at a (possibly longer) value. Configuring the former ≤ the latter ensures committer-initiated 'failed' status rows appear before PG's tx termination, giving operators bounded failure-visibility latency. The default `caller_tx_timeout_ms = 30s` reflects the realistic upper bound for user-tx duration in costing-ledger workloads (operator-driven UI clicks complete in <1s; batch jobs and MRP runs complete within seconds). Deployments with longer-running callers can raise the GUC, but should also raise `max_eject_count` proportionally if cycling pressure becomes a concern.

### 4.3 Event types

The `event_type` text discriminator routes the envelope to the right trait method. Initial event types:

- `wo_complete` — work order completion, consumes BOM components, produces finished goods
- `wo_issue` — material issue from raw to WIP
- `wo_op_move` — operation completion, moves value forward in the WO route
- `wo_scrap` — scrap from WIP, removes value
- `inv_transfer` — location transfer
- `inv_adjust` — inventory adjustment (positive or negative)
- `po_receipt` — purchase order receipt, creates new cost layer
- `so_shipment` — sales order shipment, consumes finished goods
- `cycle_count` — cycle count adjustment
- `manual_journal` — manual journal entry with cost-affecting legs

Each event type maps to a (trait_method, dispatch_logic) pair in the extension's registry. Adding a new event type requires registering its dispatch.

### 4.4 The user-tx coupling — three options in preference order

When the caller calls `ledger_enqueue` within its user-tx, what's the relationship between the user-tx commit/rollback and the envelope's eventual execution?

**Preferred: Option (C) — Caller user-tx XID stamped on envelope; committer checks pg_xact.**

The caller's `user_tx_xid` is captured at enqueue time (via `pg_sys::GetCurrentTransactionId()`, which forces XID allocation if not already assigned) and stored on the envelope in shmem. Before processing the envelope, the committer checks the user_tx_xid's status in pg_xact:

- `committed`: process the envelope normally.
- `aborted`: drop the envelope (set state = 'failed' with `error_code = 'caller_tx_aborted'`). No cost rows written. Lazy status row created if needed (see §4.4.1 below).
- `in_progress`: **eject the envelope, do NOT sleep.** Increment the staging entry's `eject_count`. If wall time since enqueue exceeds `caller_tx_timeout_ms` (default 30s — the primary wall-clock bound) OR `eject_count` exceeds `max_eject_count` (default 10000; defensive safety bound against pathological cycling, should not fire before wall-clock under normal operation), mark the envelope failed (error_code `caller_tx_timeout` or `caller_tx_eject_exhausted`). Otherwise CAS the staging entry back from `routed` to `pending`; the router re-picks on a future tick. The cycle terminates when the caller's user-tx eventually transitions to committed or aborted (or the wall-clock fires).

**The committer NEVER sleeps waiting on a caller's user-tx.** This is the single most important correctness rule in v2.1's caller-tx coupling. A committer that sleeps blocks one of the small fixed pool of BGWorkers; under high concurrency where many callers are mid-commit at any moment, all committers could be sleeping while staging and committer queues fill. The pool stalls under exactly the load v2.1 is designed to absorb. Ejection avoids this entirely: the envelope returns to the router; only that one envelope waits, and only by being re-routed. Cycle overhead is a router tick + a pg_xact check + a CAS — negligible compared to a sleeping committer.

Rationale: this is the strongest semantic. Caller can rollback freely; if they do, the work doesn't happen. No XactCallback machinery needed; no enqueue path that pushes and hopes the caller commits.

Cost: committer does a pg_xact check per envelope, which is cheap (in-memory in PG) but non-zero. Slightly delays processing of envelopes from still-in-progress user-txs.

**Second-best: Option (A) — XactCallback at PRE_COMMIT.**

The enqueue function doesn't push to shmem directly; it registers a `XactCallback(XACT_EVENT_PRE_COMMIT)` that pushes during the caller's commit. If the caller rolls back, the callback fires with `XACT_EVENT_ABORT` instead and the push is skipped.

Rationale: cleanest semantics from the caller's perspective. The envelope appears in the queue if and only if the caller committed.

Cost: pgrx's XactCallback semantics are subtle. PRE_COMMIT happens after constraint validation but before WAL fsync. If the push succeeds and then the WAL fsync fails, the caller "didn't commit" but the envelope is in the queue. The window is microsecond-scale but real. Option (C) avoids this by checking pg_xact authoritatively after the dust settles.

**Third (not recommended): Option (B) — Push immediately, ignore user-tx outcome.**

The enqueue function pushes to shmem and returns. The caller's user-tx outcome is irrelevant; the work happens regardless.

Rationale: simplest implementation. Right for fire-and-forget workflows where the caller doesn't have a user-tx (e.g., a daemon, a webhook handler).

Cost: any caller doing meaningful work in a user-tx will eventually have a rollback they care about. Option (B) means the rolled-back work happens anyway. Wrong for manual journal entries, multi-step shipment posting, and most application workflows.

**Recommendation:** implement (C) as the default. Expose a GUC `ledger.user_tx_coupling = {strict|precommit|loose}` to allow (A) and (B) opt-in for callers that need them. Tandem testing: measure pg_xact-check overhead under (C); confirm (A) actually catches PRE_COMMIT correctly under pgrx; verify (B) is operationally usable for fire-and-forget paths.

### 4.4.1 Status row creation under Option (C): two modes

Option (C) leaves open WHEN the `ledger_submission_status` row is created. Two concrete modes refine this, controlled by `ledger.status_insert_mode`:

**`caller_intx` (default):** the enqueue function INSERTs the status row with `state='queued'` inside the caller's user-tx. If the caller commits, the row exists and the committer transitions it to terminal state. If the caller aborts, the row rolls back; the staging entry persists in shmem (write was outside the user-tx); when the committer pulls the staging entry, it detects an aborted user_tx_xid and creates the row lazily with state='failed', error_code='caller_tx_aborted'. The lazy INSERT uses `ON CONFLICT (correlation_id) DO NOTHING` for idempotency under ejection-and-re-routing scenarios. **Cheapest mode AND correct under postmaster restart**, because every committed caller has a durable status row that the recovery sweep finds.

**`committer_lazy`:** no INSERT at enqueue time; committer creates rows only at terminal-state determination. **Requires `ledger.persistent_staging=on`** to be safe — without persistent staging, postmaster restart loses in-flight envelopes that have no status record, and the recovery sweep silently drops them. When persistent staging is on, the envelope itself is durable in `ledger_persistent_staging`; the recovery sweep finds envelopes via the persistent staging table and lazily creates status rows during recovery. Lowest enqueue overhead (no SPI on the enqueue path). The extension fails to load if `committer_lazy` is set with `persistent_staging=off`.

`caller_intx` is the production-recommended default. `committer_lazy` + `persistent_staging` is for deployments that want both cheapest enqueue path AND envelope durability under caller abort.

**Failure visibility under caller abort.** When a caller's user-tx aborts (network failure, app crash, explicit ROLLBACK, statement_timeout, deadlock victimization), the path to operator visibility under `caller_intx`:

1. The shmem staging entry persists with `user_tx_xid = T_caller`.
2. The router routes the entry to a committer on its next tick.
3. The committer's Step 4 checks `pg_xact_status(T_caller)`. If `aborted`: committer marks envelope failed.
4. The committer's lazy fallback runs `INSERT INTO ledger_submission_status (correlation_id, state='failed', error_code='caller_tx_aborted', ...) ON CONFLICT (correlation_id) DO NOTHING`.

Visibility latency: bounded by `min(caller_tx_timeout_ms, statement_timeout, network_keepalive_timeout)`. Default `caller_tx_timeout_ms = 30s` assumes deployments configure `statement_timeout >= 30s` (or unlimited). With these defaults, typical latency is one committer cycle (tens of ms); worst case is tens-of-seconds.

The ON CONFLICT DO NOTHING semantics make the lazy INSERT idempotent under re-routing: if an envelope is ejected and routed to a second committer that also observes the aborted user-tx, both committers may attempt the lazy INSERT. The PK conflict is silent and safe.

**Why `caller_subtx` is not a supported mode.** Earlier drafts of this spec defined a third mode, `caller_subtx`, intended to deliver status-row durability independent of the caller's user-tx outcome. Implementation discovered that PG sub-transactions are savepoints, not autonomous transactions: `BeginInternalSubTransaction` + `ReleaseCurrentSubTransaction` folds the sub-tx's writes into the parent's pending state. Row visibility still requires parent commit; row is still lost on parent abort. The mode delivered error isolation on the status INSERT (failure of the INSERT doesn't abort the caller's user-tx) but NOT abort survival.

Analysis of realistic failure modes for the status INSERT (caller bugs producing constraint violations, system OOM, table corruption) found the survival behavior insufficiently justified to retain a distinct mode. For callers requiring true durability under caller abort, use `committer_lazy` with `persistent_staging=on`. The persistent_staging row is WAL-logged within the caller's user-tx; on caller commit, it's durable; on caller abort, it's rolled back along with the caller's work, which is the correct semantics — an aborted submission was never durably submitted.

True autonomous transactions (via `dblink` or `pg_background`) could deliver abort-survival but at the cost of an out-of-process round-trip per enqueue, which is incompatible with v2.1's throughput goals. The option is documented as a future consideration if a use case justifies the cost.

**Eject cleanup interaction with the committer's transaction commit (Step 5 / §7):** the committer's post-commit cleanup iterates ALL staging entries originally assigned to the SuperBatch, attempting `CAS valid: 3 → 0` on each. For ejected entries, this CAS fails because the entry's valid is now `1 (pending)` (the eject CAS already transitioned it). The cleanup MUST treat CAS-failure as "this entry was ejected, leave it alone — do not free its arena blocks." Freeing the arena of an ejected entry would dangle the router's pointers when it re-picks. This CAS-failure-skip semantics is the load-bearing rule that makes ejection safe.

### 4.5 Backpressure: synchronous enqueue with timeout

If the staging shmem queue is full, `ledger_enqueue` blocks for up to `queue_full_timeout_ms` (default 5000ms). The wait uses a condition variable signaled by the router when it drains a slot.

If the timeout elapses with no space available, the function throws:

```
ERROR: 53200 (ERRCODE_OUT_OF_MEMORY) / 53300 (ERRCODE_INSUFFICIENT_RESOURCES) / 57P03 (ERRCODE_CANNOT_CONNECT_NOW)
DETAIL: Ledger staging queue full; retry after backpressure clears.
HINT: Increase ledger.staging_queue_size or reduce concurrent enqueue rate.
```

Choice of ERRCODE matters because PG client libraries treat them differently for retry decisions. `53300` is conventional for "resource pressure"; `57P03` is conventional for "system not ready"; clients typically retry the latter automatically. The right choice depends on whether the extension wants callers to retry transparently. Default to `53300` and document.

The caller's user-tx aborts on this error (per standard PG transaction semantics on uncaught errors). The application tier sees the error and can decide to retry, fail the user, queue locally, etc.

**Trade-off:** synchronous-enqueue couples the caller's user-tx to staging queue capacity. Under sustained overload, callers see errors. This is the right behavior — silent overflow to disk or hard drop are both worse. The application tier's job is to handle the error reasonably.

### 4.6 Caller atomicity within an envelope

One envelope is one transactional unit. If a caller wants to submit 5 WO completions as a unit:

- **Right way:** one envelope with `event_type = 'wo_complete_multi'` (a registered multi-WO event type) carrying all 5 WOs in the payload and the union of all their pool_keys.
- **Wrong way:** 5 separate `ledger_enqueue` calls. The router treats these as 5 envelopes and may pack them into different SuperBatches; partial success is possible.

The router NEVER groups multiple envelopes into a single atomic unit. If the caller wants atomicity across multiple WOs, they bundle them at the envelope level.

The trait must support multi-WO event types. design-v2's trait protocol handles this via `plan_apply` taking a batch of events; v2.1 reuses it.

### 4.7 Durability: the `durable_queue` parameter

The `durable_queue` parameter on `ledger_enqueue` controls per-envelope durability. The choice is per-envelope, not per-deployment — different callers in the same database can mix durable and non-durable submissions.

**Two modes:**

- **`durable_queue = false` (default).** Shmem-only staging. The enqueue writes the envelope to the shared-memory staging queue and returns. The write is non-WAL; it does not fsync. If postmaster crashes after the function returns but before the committer commits the cost rows, the envelope is lost. The submission_status row (if it exists under `caller_intx`) is transitioned to `state='failed'`, `error_code='postmaster_restart_loss'` by the recovery sweep (§9.4). The caller observes this terminal state by polling `ledger_submission_status`. Cheapest enqueue path; the right choice when re-submission is cheap and the application can monitor progress.

- **`durable_queue = true`.** Persistent staging. The enqueue path additionally INSERTs a row into `ledger_persistent_staging` (§1.1) within the caller's user-tx. The row is WAL-logged and survives postmaster restart. The INSERT rides on the caller's normal commit fsync — no additional fsync is added to the enqueue critical path beyond what the caller's own transaction was already going to do. After postmaster restart, the §9.4 recovery sweep replays uncompleted persistent staging rows back into the shmem queue. Envelopes from committed callers are recovered; the work eventually happens.

**Function behavior under `durable_queue=true`:**

The deployment must have `ledger.persistent_staging = on`. If not, the function raises `ERRCODE_FEATURE_NOT_SUPPORTED` immediately. The extension does NOT silently downgrade a `durable_queue=true` request to non-durable; callers can rely on the parameter meaning what it says.

**Caller guidance.** The choice is workload-dependent:

- **Batch operator-driven workflows** (WO completions, EOD posting, bulk receipts, cycle counts): `durable_queue=false` is typically appropriate. The operator can monitor progress via `ledger_submission_status` polling and re-submit failed envelopes if a crash occurs. The throughput benefit of shmem-only staging is meaningful at scale.

- **Singular high-stakes interactions** (manual journal entries, financial-period closing postings, anything where the caller needs an authoritative success/failure outcome before walking away from the screen): `durable_queue=true`. After enqueue, the caller polls `ledger_submission_status` for terminal state. A postmaster restart mid-polling doesn't lose the work; the recovery sweep re-enqueues it, and the poll eventually sees the outcome.

The application tier may wrap this in a `ledger_enqueue_and_wait(...)` helper combining `durable_queue=true` with a polling loop. That helper is application-layer code, not extension machinery. The extension's contract stops at "envelope durable; terminal state observable via polling."

**Cost of durability:**

- Per-enqueue: one additional INSERT into `ledger_persistent_staging`, WAL-logged within the caller's user-tx. No extra fsync beyond what the caller was doing.
- Per-envelope at the committer: persistent_staging row transitions `staged → completed` directly on the hot path. The transition is bundled into Step 5's bulk UNNEST UPDATE (one extra UPDATE statement when at least one durable envelope is present in the SuperBatch). The `in_shmem` state is reserved for the postmaster-restart recovery sweep as a diagnostic marker; hot-path processing skips it.
- Postmaster restart: recovery time scales linearly with in-flight persistent staging row count. For workloads with retention-bounded GC, this is bounded by `persistent_staging_gc_retention_hours × peak_durable_enqueue_rate`.

Bake-off in §13.4 measures the throughput delta as a function of `durable_queue` request rate.

**Sync-wait is not built in.** Some designs add a CV-block on the enqueue path to give the caller a synchronous "wait for terminal state" semantic. v2.1 explicitly does NOT do this. Reasons: (1) the committer pool is small; a synchronous-wait caller holds a connection-pool slot for the full pipeline duration, degrading concurrency under load; (2) it adds a parallel result-delivery channel (result slots, CVs, orphan recovery for waiting callers) that duplicates `ledger_submission_status`; (3) polling is sufficient and lives at the application tier where retry/timeout policy belongs. Callers wanting sync semantics poll after enqueue.

---

## 5. Shmem Layout

### 5.1 Two queues

The extension maintains two shmem queues:

- **Staging queue**: where `ledger_enqueue` writes. The router reads from this.
- **Committer queue**: where the router writes assembled SuperBatches. Committers read from this.

Separating staging from committer queues lets the router buffer envelopes for window-packing without blocking enqueue. The router can ingest 1000 envelopes from the staging queue and produce 20 SuperBatches into the committer queue; enqueue continues during this work.

### 5.2 Staging queue layout

```rust
const STAGING_QUEUE_SIZE: u32 = 65536;  // GUC-tunable

#[repr(C, align(64))]
pub struct StagingQueue {
    pub head: AtomicU32,
    pub tail: AtomicU32,
    pub lock_tranche_id: u32,
    pub _pad: [u8; 4],
    pub next_request_seq: AtomicU64,
    pub backpressure_cv_tranche_id: u32,
    pub _pad2: [u8; 4],
    // entries[STAGING_QUEUE_SIZE] follow
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
    pub sku_pool_count: u16,           // SKU pool_keys count; committer dedups union across batch
    pub wip_pool_count: u16,           // WIP pool_keys count; committer dedups union across batch
    pub sku_pool_keys_offset: u32,     // arena offset of (sku_id, location_id) array
    pub wip_pool_keys_offset: u32,     // arena offset of (work_order_id, operation_id) array
    pub enqueued_at_micros: u64,
    pub backend_pid: i32,
    pub superbatch_id: AtomicU64,      // Set with Release before CAS valid: 2→3.
                                       // Read with Acquire after observing valid==3.
                                       // Data-before-flag pattern; see §6.4.
    pub eject_count: AtomicU32,        // Per-envelope eject cycle counter; bounded by
                                       // ledger.max_eject_count to terminate pathological loops.
                                       // Widened to u32 so max_eject_count can sit comfortably
                                       // above the wall-clock bound (~1500-6000 ejects at
                                       // 30s/5-20ms-per-cycle).
    pub _pad2: [u8; 6],
}
```

The 5-state machine on `valid`:

- `0 empty` — slot is free
- `1 pending` — caller pushed, router hasn't claimed yet
- `2 processing` — router has claimed for SuperBatch assembly (in router-local memory)
- `3 routed` — router has pushed the containing SuperBatch to the committer queue; the staging entry is awaiting commit confirmation
- `4 abandoned` — caller user-tx aborted (Option C check) OR router or committer abandoned the slot

Transitions:
- `0 → 1`: enqueue. Under staging queue LWLock.
- `1 → 2`: router claim. Under staging queue LWLock.
- `2 → 1`: router death recovery sweep (claim reverted because router died before pushing).
- `2 → 3`: router successfully pushed SuperBatch to committer queue. Under staging queue LWLock for atomicity with router's other work.
- `3 → 0`: committer confirmed commit (or replayed, or failed); slot returns to pool. Done by the committer's post-commit path.
- `* → 4`: abandonment for caller-tx-abort or invariant violation.

The transitions `2 → 3` and `3 → 0` are the critical ones for the no-double-execution guarantee. See §6.4 for the router's atomic push-then-CAS ordering.

### 5.3 Committer queue layout

```rust
const COMMITTER_QUEUE_SIZE: u32 = 8192;  // GUC-tunable

#[repr(C, align(64))]
pub struct CommitterQueue {
    pub head: AtomicU32,
    pub tail: AtomicU32,
    pub lock_tranche_id: u32,
    pub _pad: [u8; 4],
    pub next_superbatch_id: AtomicU64,
    // entries[COMMITTER_QUEUE_SIZE] follow
}

#[repr(C)]
pub struct CommitterQueueEntry {
    pub valid: AtomicU8,              // 0=empty, 1=ready, 2=in_flight, 3=completed
    pub _pad: [u8; 7],
    pub superbatch_id: u64,
    pub envelope_count: u16,
    pub staging_entry_offsets: u32,       // arena offset of staging entry indices array
    // Pool keys are stored as two separate sorted, deduplicated arrays — one per lock
    // domain (§2). The committer's Step 2 acquires SKU locks first, then WIP locks,
    // each in lex order within their own domain.
    pub sku_pool_keys_offset: u32,        // arena offset of sorted dedup'd SKU pool_keys
    pub sku_pool_keys_count: u16,
    pub wip_pool_keys_offset: u32,        // arena offset of sorted dedup'd WIP pool_keys
    pub wip_pool_keys_count: u16,
    pub committer_slot: AtomicU32,        // Index into the extension's own
                                          // CommitterIdentityRegistry shmem array
                                          // (see §5.5). Combined with committer_token,
                                          // uniquely identifies a running committer process —
                                          // safe against OS PID recycling.
                                          // Sentinel 0xFFFFFFFF = "no committer claimed yet."
    pub committer_token: AtomicU64,       // Stable per-process identity token written by
                                          // the committer at registration time. Computed
                                          // from (postmaster_start_time_ns, MyProcPid) so
                                          // that no two committers across the postmaster's
                                          // lifetime can share a token even if PIDs recycle.
                                          // A stale token in the queue entry unambiguously
                                          // means the original committer is gone.
    pub committer_acquired_at_ns: AtomicU64,
    pub committer_tx_id: AtomicU64,    // assigned at Step 2; for orphan recovery
    pub enqueued_at_micros: u64,
}
```

**Note on committer identity and liveness.** Raw OS PIDs are unreliable for liveness checks in containerized or high-process-churn environments — a recycled PID can falsely report a dead committer as alive. PG's internal `BackgroundWorkerData` array (`bgworker_internals.h`) holds slot+generation info that would solve this but is NOT a public extension API — extensions cannot link against it. The extension therefore maintains its own committer identity registry in the shmem block it allocates during `_PG_init` (see §5.5 CommitterIdentityRegistry).

Each committer, on BGWorker startup, atomically claims a registry slot and writes its `(pid, token, active)` where `token = hash(postmaster_start_time_ns, MyProcPid)`. The token is unique across the postmaster's lifetime: `postmaster_start_time_ns` advances monotonically across restarts, and within one postmaster's lifetime PIDs are not reused for live processes simultaneously.

CommitterQueueEntry stores `(committer_slot, committer_token)` at claim time. Liveness check:

1. Read `(committer_slot, committer_token)` from the queue entry.
2. Read `CommitterIdentityRegistry[committer_slot]`.
3. If `registry_slot.active == true` AND `registry_slot.token == committer_token` → committer is alive.
4. Otherwise (slot inactive, or token mismatch indicating slot was reused by a different process) → committer is dead.

The registry is small (one entry per `max_committers` GUC, bounded at startup), uses only public PG shmem APIs, and avoids private symbol leaks. On BGWorker exit, the committer's `at-exit` callback marks its registry slot `active = false`; on subsequent (re)start, the new committer claims the slot, writes its new token, and sets active = true. Recovery sweeps and audits use the public liveness check above instead of `kill(pid, 0)`.

Transitions:
- `0 → 1`: router push.
- `1 → 2`: committer claim via CAS — atomically writes (committer_slot, committer_token) for the claiming worker. Sets `committer_acquired_at_ns` immediately after.
- `2 → 3`: committer post-commit. Releases staging entries (3 → 0 on staging queue).
- `2 → 1`: orphan recovery (committer's (slot, token) no longer identifies a live worker; another committer reclaims by CAS).
- `3 → 0`: slot returns to pool.

### 5.4 Spillover arena

JSON payloads, pool_keys arrays, and staging_entry_offset arrays for SuperBatches all need variable-length storage. A shmem-backed arena with a freelist allocator:

```rust
const SPILLOVER_ARENA_MB: u32 = 256;  // GUC-tunable

#[repr(C, align(64))]
pub struct SpilloverArena {
    pub total_size: u32,
    pub freelist_head: AtomicU32,
    pub lock_tranche_id: u32,
    pub _pad: [u8; 4],
    // arena bytes follow
}
```

Block sizes are quantized (16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 bytes) to keep the freelist simple. Allocations round up to the next block size. Free blocks are returned to the per-size freelist.

**Flagged concern (§14 A7):** the freelist allocator can fragment under sustained mixed workload. Alternative: a slab allocator with explicit pool sizes per call site (payload pool, pool_keys pool, etc.). More tuning surface but predictable. Test the freelist first; switch to slab if fragmentation becomes a problem.

### 5.5 Shmem allocation at startup

All shmem is sized at startup via `RequestAddinShmemSpace` in `_PG_init`. GUCs are read at startup; changes require postmaster restart:

- `ledger.staging_queue_size` (default 65536)
- `ledger.committer_queue_size` (default 8192)
- `ledger.spillover_arena_mb` (default 256)
- `ledger.queue_full_timeout_ms` (default 5000)
- `ledger.committer_lease_ms` (default 100; should be tuned per `max(100, 10 × fsync_p99_ms)`)
- `ledger.committer_count` (default 4)
- `ledger.router_window_size` (default 1000)
- `ledger.batch_size_max` (default 50 envelopes)
- `ledger.batch_window_us` (default 500)
- `ledger.user_tx_coupling` (default 'strict' = Option C from §4.4)
- `ledger.status_insert_mode` (default 'caller_intx'; see §4.4.1)
- `ledger.max_eject_count` (default 10000)
- `ledger.caller_tx_timeout_ms` (default 30000)
- `ledger.snapshot_layer_limit_per_pool` (default 1000)
- `ledger.webhook_max_attempts` (default 10)
- `ledger.webhook_backoff_base_ms` (default 100)

Several of these are tunable per workload and the bake-off (per design-v2's methodology) determines optimal values.

**CommitterIdentityRegistry.** Shmem array sized to `ledger.committer_count + N_reserved` slots (N_reserved is a small headroom for short-lived audit workers if any). Allocated in `_PG_init` as part of the extension's shmem block:

```rust
#[repr(C, align(64))]
pub struct CommitterIdentitySlot {
    pub active: AtomicBool,         // True when a committer process holds this slot.
    pub pid: AtomicI32,             // The committer's OS PID (for diagnostics only;
                                    // not load-bearing for liveness).
    pub token: AtomicU64,           // Stable identity token: hash(postmaster_start_time_ns,
                                    // MyProcPid). Unique across the postmaster's lifetime.
    pub last_heartbeat_ns: AtomicU64, // Optional: committers may write this periodically
                                    // for diagnostics. Not used for liveness — liveness is
                                    // (active && token-match).
}

#[repr(C, align(64))]
pub struct CommitterIdentityRegistry {
    pub slot_count: u32,
    pub _pad: [u8; 4],
    pub slots: [CommitterIdentitySlot; MAX_COMMITTERS], // bounded by committer_count + N_reserved
}
```

Committer BGWorker startup sequence:
1. Compute `my_token = hash(GetPostmasterStartTime(), MyProcPid)`.
2. Scan registry for a slot where `active.compare_exchange(false, true, Acquire) == Ok(_)`.
3. On success, write `pid = MyProcPid`, `token = my_token`, then continue normal startup.
4. On failure (registry full): log a fatal error and exit; this indicates committer_count was misconfigured.
5. Register an `at-exit` callback that sets `active = false` on the claimed slot (leaving pid and token in place for diagnostic purposes; the active=false signals the slot is reclaimable).

The (slot, token) pair is what the CommitterQueueEntry stores; the liveness check is the public API any backend can call without touching PG internals.

---

## 6. The Router

### 6.1 Single BGWorker, postmaster-restartable

The Router is a single PostgreSQL Background Worker (BGWorker) registered in `_PG_init`. Single thread by design: dual-routing introduces packing races and partition disagreements. Single threaded routing is fast enough (in-memory array comparisons, no SPI) to outpace I/O-bound committers.

If the Router panics or OOMs, the postmaster restarts it per standard bgworker policy. On restart, the recovery sweep (§6.4) runs before the Router resumes normal duty.

### 6.2 The packing loop

```
loop {
    // 1. Read pending envelopes from staging queue
    let window = staging_queue.read_pending_window(router_window_size);
    if window.is_empty() {
        wait_with_timeout(batch_window_us);
        continue;
    }

    // 2. Affinity grouping: union-find on pool_key overlap
    //    Build pool_key -> Vec<envelope_idx> map
    let mut pool_to_envelopes: HashMap<PoolKey, Vec<usize>> = HashMap::new();
    for (idx, candidate) in window.iter().enumerate() {
        for key in candidate.sku_pool_keys.iter().chain(candidate.wip_pool_keys.iter()) {
            pool_to_envelopes.entry(*key).or_default().push(idx);
        }
    }

    let mut uf = UnionFind::new(window.len());
    for envelopes_sharing_key in pool_to_envelopes.values() {
        if envelopes_sharing_key.len() >= 2 {
            let head = envelopes_sharing_key[0];
            for &other in &envelopes_sharing_key[1..] {
                uf.union(head, other);
            }
        }
    }

    // 3. Collect connected components and sort by min(request_seq) for fairness
    let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
    for idx in 0..window.len() {
        let root = uf.find(idx);
        groups.entry(root).or_default().push(idx);
    }

    let mut ordered_groups: Vec<Vec<usize>> = groups.into_values().collect();
    for group in &mut ordered_groups {
        group.sort_by_key(|&i| window[i].request_seq);  // arrival order within group
    }
    ordered_groups.sort_by_key(|group| window[group[0]].request_seq);  // oldest group first

    // 4. Each group becomes one or more SuperBatches (split at batch_size_max)
    for group in &ordered_groups {
        for chunk in group.chunks(batch_size_max) {
            let mut superbatch = SuperBatch::new();
            for &candidate_idx in chunk {
                if staging_queue.claim(window[candidate_idx].staging_idx) {
                    superbatch.add(&window[candidate_idx]);
                }
                // CAS failure (defensive): the envelope was claimed by another
                // process or already advanced. The remaining envelopes in this
                // chunk still belong to the same connected component; if the
                // missing envelope ended up in another SuperBatch, FOR UPDATE
                // serialization handles the cross-SB contention as fallback.
            }

            if superbatch.is_empty() { continue; }

            // 5. Compute deduplicated, lex-sorted lock-set union for this SuperBatch
            let sku_keys = dedup_sorted_union(&superbatch, |e| e.sku_pool_keys);
            let wip_keys = dedup_sorted_union(&superbatch, |e| e.wip_pool_keys);

            // 6. Publish to committer queue with memory-ordered superbatch_id write
            committer_queue.push(superbatch, sku_keys, wip_keys);
            staging_queue.mark_routed(superbatch.staging_indices());
        }
    }
}
```

The packing loop **actively groups** envelopes by pool-key overlap. Envelopes that share any pool_key (transitively) are routed to the same SuperBatch; independent groups become independent SuperBatches that committers can run in parallel.

**Complexity.** Per-tick cost is dominated by:
- Window read: O(W) atomic loads.
- Pool-key map construction: O(W × K) hashmap insertions where K is average pool_keys per envelope.
- Union-find: O(W × K × α(W)) where α is inverse Ackermann (effectively constant).
- Component collection and sort: O(W log G) where G is number of components.

For W=1000 (default router_window_size) and K=15 (average for `wo_complete` with 5 components + WIP + output), per-tick work is ~2-3ms. Router ceiling: 300-500 ticks/sec at full window saturation, ~300K-500K envelopes/sec. See design §13.3 throughput analysis.

### 6.3 The fairness rule (defensive — should not arise in single-router)

Under affinity grouping in §6.2, the oldest envelope's connected component is dispatched first in each tick (groups sorted by min(request_seq); within a group, envelopes sorted by request_seq). No envelope can starve. This subsection is retained for two reasons:

1. **Future multi-router design.** If the router is later parallelized, two routers racing on CAS could repeatedly skip the same entry. The `router_starvation_threshold_ticks` GUC retains its meaning: an entry skipped for this many consecutive ticks is forcibly claimed by whichever router observes the count first. Single-router PoC sets this defensively but does not exercise it.

2. **Edge cases under shutdown/restart sequences.** If the router is restarting and there's a brief window where multiple stale processes attempt to scan, the threshold provides a backstop. Implementation can use a simpler test (timestamp on first observation rather than tick count) if preferred.

There is no hot-pool fairness concern under affinity grouping. A hot pool (many envelopes touching the same SKU) produces one large connected component, which the router splits into chunks of `batch_size_max` and dispatches sequentially. Each chunk goes to a committer; the committers serialize via PG row locks on `pool_locks`. Throughput on the hot pool is single-committer rate × batch_size_max envelopes per chunk — bounded but not blocked. Non-hot work runs in parallel SuperBatches on other committers.


### 6.4 Router death and recovery

The Router runs an explicit transition between three atomic shmem operations per envelope:

1. **Local SuperBatch assembly:** scan window, build SuperBatch in router-local memory. Staging entries marked `pending → processing` as they're claimed.
2. **Push to committer queue:** insert SuperBatch into committer queue (queue entry valid: 0 → 1, ready).
3. **For each staging entry in the SuperBatch:**
   - Store `superbatch_id` on the staging entry with **Release** memory ordering: `staging.superbatch_id.store(sb_id, Release)`.
   - CAS staging entry valid: 2 → 3 (routed). On success, the prior `superbatch_id` store is visible to any reader that observes valid=3 via Acquire.

**Memory ordering is load-bearing.** The Release store of `superbatch_id` before the CAS, paired with Acquire loads in the recovery sweep, is a classic data-before-flag pattern. Reversing the ordering or weakening the memory order creates a window where a reader observes `valid=3` but reads stale `superbatch_id=0`, leading to incorrect "never pushed" classification.

**The recovery sweep on Router boot:**

```
For each staging entry where staging.valid.load(Acquire) == 2 (processing):
  superbatch_id = staging.superbatch_id.load(Acquire)
  if superbatch_id == 0:
    // Router died mid-pack; never assigned this entry to a SuperBatch.
    CAS staging.valid: 2 → 1 (pending). Re-route on next tick.
    continue
  // superbatch_id != 0: was assigned to a SuperBatch. Look it up.
  qe = lookup_committer_queue_entry(superbatch_id)
  if qe is None or qe.valid == 0:
    // SuperBatch was never actually pushed before router died.
    CAS staging.valid: 2 → 1 (pending). Re-route.
  elif qe.valid in {1 (ready), 2 (in_flight)}:
    // Live SuperBatch; committer will process and clean up.
    Leave staging entry alone.
  elif qe.valid == 3 (completed):
    // The committer committed but may or may not have completed Step 14
    // staging cleanup before any subsequent failure.
    if committer_alive(qe.committer_slot, qe.committer_token):
      // Committer is still cleaning up (or just did); §13 periodic audit
      // catches genuine leaks.
      Leave staging entry alone.
    else:  // committer is dead
      // Cost rows are durable; committer died before staging cleanup.
      // Sweep takes ownership of cleanup.
      For each linked staging entry in this SuperBatch:
        CAS staging.valid: 2 → 0 (empty)
        Free staging entry's arena blocks (payload, pool_keys)
      Free the CommitterQueueEntry's OWN arena blocks (staging_entry_offsets,
        sorted sku_pool_keys, sorted wip_pool_keys — owned by queue entry,
        not by staging entries; separate freeing required).
      CAS queue entry: 3 → 0 (slot reusable)
      Idempotently confirm submission_status rows are 'committed' (the dead
        committer likely already wrote them pre-death; use ON CONFLICT to
        no-op if already correct).
```

The **completed-but-committer-died case** is also handled independently by the periodic audit (§13). Two paths converge on the same recovery for defense in depth.

**The double-execution window:** between step 2 (push to committer queue) and step 3 (Release-store + CAS to routed), the router could die. The committer might pick up and execute the SuperBatch. The dedup-lookup at Step 2.5 (§7) is the safety net for any case where the same envelope might end up in two SuperBatches due to a recovery edge — duplicate `(issue_id, method)` pairs are filtered out before plan_apply runs.

The router's push-then-Release-store-then-CAS ordering is safe under all crash interleavings. The data-before-flag invariant ensures the recovery sweep never observes inconsistent state.

---

## 7. Committer Execution Pipeline

### 7.1 Overview

A committer is any backend that elects itself committer for a SuperBatch via CAS on the committer queue entry's claim slot. Committers are a generic worker pool — any backend connected to PG can act as a committer when not busy with the application's work, though in practice committers are typically dedicated BGWorkers. The claim atomically writes the committer's (committer_slot, committer_token) pair into the queue entry, where committer_slot is the committer's index in the extension's CommitterIdentityRegistry (§5.5) and committer_token is its stable per-process identity. The pair makes liveness checks independent of OS PID recycling and avoids reaching into PG's private BackgroundWorkerData symbols.

The pipeline holds the FOR UPDATE locks acquired in Step 2 through all of Steps 2.5, 3, 4, and 5 (the bulk INSERT). The locks release on top-level transaction commit at the end of Step 5. This is the load-bearing invariant for sole-writer correctness — see §3.2 I-sole-writer.

### 7.2 Step 1 — Batch retrieval and lexicographical sorting

```rust
fn step1_retrieve_and_sort(superbatch: &CommitterQueueEntry) -> SortedPoolKeys {
    let pool_keys = read_assembled_pool_keys(superbatch);
    let mut sku_keys: Vec<SkuPoolKey> = pool_keys.iter()
        .filter_map(|k| k.as_sku())
        .collect();
    let mut wip_keys: Vec<WipPoolKey> = pool_keys.iter()
        .filter_map(|k| k.as_wip())
        .collect();
    sku_keys.sort_unstable_by(|a, b| (a.sku_id, a.location_id).cmp(&(b.sku_id, b.location_id)));
    wip_keys.sort_unstable_by(|a, b| (a.work_order_id, a.operation_id).cmp(&(b.work_order_id, b.operation_id)));
    SortedPoolKeys { sku_keys, wip_keys }
}
```

The pool_keys arrays read here are the **deduplicated, lex-sorted union** of all envelopes' pool_keys in the SuperBatch, as written by the router in §6.2 step 3. Overlapping envelopes contributed each unique pool_key once; the resulting arrays carry no duplicates. Step 1's sort is performed by the router; the committer reads pre-sorted arrays.

### 7.3 Step 2 — Deterministic lock acquisition

```rust
fn step2_acquire_locks(sorted: &SortedPoolKeys) -> Result<()> {
    StartTransactionCommand();
    SetTransactionIsolationLevel(READ_COMMITTED);
    // Each SuperBatch runs in its own top-level transaction:
    //   StartTransactionCommand → ... → CommitTransactionCommand.
    // - One WAL fsync per SuperBatch (B1 amortization holds).
    // - committer_tx_id is a top-level XID; pg_xact_status returns its
    //   actual final state (committed/aborted/in_progress), unambiguously
    //   queryable by orphan-recovery (§9.1).
    // - Cost rows are durable at CommitTransactionCommand return; subsequent
    //   committer crashes leave durable cost rows intact, enabling the
    //   post-commit-pre-cleanup recovery path (§9.1 committed branch).
    //
    // READ COMMITTED is load-bearing for causal-chain correctness: when this
    // committer's FOR UPDATE blocks behind another committer's FOR UPDATE for
    // the same pool, PG releases the lock at the holder's commit time. The
    // committer's subsequent snapshot hydration SELECT (Step 3) takes a FRESH
    // snapshot at the SELECT's execution time, which is AFTER the holder's
    // commit — so it observes the holder's committed work. Stricter isolation
    // (REPEATABLE READ, SERIALIZABLE) would freeze the snapshot at transaction
    // start and the hydration SELECT would NOT see the upstream committer's
    // rows. This silently breaks causal ordering and produces spurious
    // InsufficientInventory errors at low inventory levels. The implementation
    // MUST verify the transaction runs at READ COMMITTED.
    // See invariant I-causal-snapshot-observability (§3.2).
    //
    // An earlier draft used BeginInternalSubTransaction inside a long-lived
    // BGWorker parent transaction. That model was unsuitable: subtransaction
    // release does not flush WAL, the parent tx would accumulate XIDs
    // indefinitely (xmin horizon bloat, wraparound risk), and on BGWorker
    // crash the postmaster would abort the parent and wipe all released
    // subtransactions' work. Top-level-per-SuperBatch avoids all three.

    // Lazy-create lock rows via singleton-loop in sorted lex order.
    // POSTGRESQL DOES NOT GUARANTEE the row-acquisition order of
    // `INSERT ... UNNEST(...) ON CONFLICT DO NOTHING` against concurrent
    // writers. UNNEST emits rows in array order but the executor may write
    // them in any order, and tuple-level exclusive locks are taken as rows
    // are written. If two committers concurrently INSERT overlapping
    // pool_key sets, they can deadlock during the bulk INSERT phase, before
    // either reaches the FOR UPDATE singleton loop. Lazy creation therefore
    // uses the same singleton-loop pattern as FOR UPDATE acquisition:
    for (sku_id, location_id) in sorted.sku_keys {
        spi_execute_prepared(
            "INSERT INTO pool_locks (sku_id, location_id) VALUES ($1, $2)
             ON CONFLICT DO NOTHING",
            (sku_id, location_id)
        );
    }
    for (work_order_id, operation_id) in sorted.wip_keys {
        spi_execute_prepared(
            "INSERT INTO wip_pool_locks (work_order_id, operation_id) VALUES ($1, $2)
             ON CONFLICT DO NOTHING",
            (work_order_id, operation_id)
        );
    }

    // Acquire locks in singleton loop, sorted lex order. PostgreSQL does NOT
    // guarantee that `SELECT ... ORDER BY ... FOR UPDATE` locks rows in
    // ORDER BY order — the planner may lock rows during an index or
    // sequential scan BEFORE applying the sort node. For cross-SuperBatch
    // deadlock avoidance under the affinity-grouping split fallback (where
    // two committers process chunks of the same connected component and
    // overlap on pool_keys), the lock acquisition order MUST be
    // ironclad-deterministic. Singleton SPI calls cannot be reordered:
    for (sku_id, location_id) in sorted.sku_keys {
        spi_execute_prepared(
            "SELECT 1 FROM pool_locks WHERE sku_id = $1 AND location_id = $2 FOR UPDATE",
            (sku_id, location_id)
        );
    }
    for (work_order_id, operation_id) in sorted.wip_keys {
        spi_execute_prepared(
            "SELECT 1 FROM wip_pool_locks WHERE work_order_id = $1 AND operation_id = $2 FOR UPDATE",
            (work_order_id, operation_id)
        );
    }

    // Prepared-statement requirement. The four singleton-loop queries above
    // MUST be prepared once per committer (during init) and reused. Without
    // preparation, per-call SPI cost is ~50μs (re-parse + re-plan on each
    // invocation). With preparation, per-call cost is ~5-10μs. The doubled
    // singleton-loop cost (2(P+Q) SPI calls instead of 4 bulk statements)
    // is the price of deterministic lock-row creation AND lock acquisition.
    // See §13.3 B2 for the throughput analysis.

    Ok(())
}
```

On serialization_failure: AbortCurrentTransaction, retry the SuperBatch once with a fresh StartTransactionCommand. On second failure: escalate (orphan the SuperBatch back to state 1; another committer can attempt). This shouldn't happen given lex-locking but is the defensive backstop.

### 7.4 Step 2.5 — Dedup-lookup

The dedup-lookup verifies that the envelopes in this SuperBatch are not replays of previously-processed work. Lock has already been acquired in Step 2, so no concurrent committer can write to the relevant pools.

```rust
fn step2_5_dedup_lookup(superbatch: &SuperBatch) -> DedupResult {
    // Two dedup paths run in parallel; either hit means the event was
    // previously processed and should be replayed from existing rows.

    // (a) Consumption-side dedup. Collect (issue_id, method_used) pairs
    //     from consumption events.
    let mut consumption_pairs: Vec<(i64, &str)> = Vec::new();
    for env in &superbatch.envelopes {
        for event in env.consumption_events() {
            consumption_pairs.push((event.issue_id, event.method_used));
        }
    }

    let consumption_hits: HashSet<(i64, String)> = spi_execute(
        "SELECT issue_id, method_used FROM cost_layer_depletions
         WHERE (issue_id, method_used) IN (
             SELECT u.issue_id, u.method_used
               FROM UNNEST($1::bigint[], $2::text[]) AS u(issue_id, method_used)
         )
         UNION
         SELECT issue_id, method_used FROM cost_consumptions
         WHERE (issue_id, method_used) IN (
             SELECT u.issue_id, u.method_used
               FROM UNNEST($1::bigint[], $2::text[]) AS u(issue_id, method_used)
         )",
        consumption_pairs
    );

    // (b) Receipt-side dedup. Collect correlation_ids from receipt events
    //     (PoReceipt, WoComplete output, InvAdjust with positive qty).
    //     Layers don't carry issue_id; their natural dedup key is
    //     correlation_id (one envelope produces its layers exactly once).
    let receipt_correlation_ids: Vec<Uuid> = superbatch.envelopes.iter()
        .filter(|env| env.has_receipt_events())
        .map(|env| env.correlation_id)
        .collect();

    let receipt_hits: HashSet<Uuid> = spi_execute(
        "SELECT correlation_id FROM cost_layers
         WHERE correlation_id = ANY($1::uuid[])",
        receipt_correlation_ids
    );

    DedupResult {
        replayed_consumptions: consumption_pairs.iter()
            .filter(|k| consumption_hits.contains(k)).collect(),
        replayed_receipts: receipt_correlation_ids.iter()
            .filter(|c| receipt_hits.contains(c)).collect(),
        to_plan: /* events not matched by either path */,
    }
}
```

For replayed events, the committer reads the existing rows (via a follow-up SELECT in Step 3's hydration query, scoped to the replayed issue_ids and replayed correlation_ids) and uses their values as the event's result. No re-INSERT, no plan_apply call. The terminal state delivered to the caller for the envelope is `replayed`.

The query uses `(issue_id, method_used) IN (SELECT * FROM UNNEST(...))` — paired matching, NOT `issue_id = ANY(...) AND method_used = ANY(...)` which would be cartesian.

**Flagged concern (§14 A1, reprised):** the depletion table's UNIQUE constraint is `(issue_id, method_used, layer_id)`, allowing the same `(issue_id, method_used)` across different layers. The dedup-lookup matches on `(issue_id, method_used)` regardless of layer, so any depletion row for the issue is enough to flag it as replayed. This is correct for the "caller submitted the same FIFO consumption twice" case (the issue spans multiple layers; first row found marks the issue replayed). But it's vulnerable to the caller-bug case where issue_id is reused across pools (different layer_ids in different pools); dedup-lookup sees the first pool's depletion rows and treats the second pool's submission as a replay.

The fix requires the UNIQUE constraint to encode the pool identity, or the dedup-lookup to additionally filter by the locked pool_keys. Two alternatives:

- **(A)** Add `sku_id, location_id` to the depletion table's UNIQUE constraint: `UNIQUE (issue_id, method_used, sku_id, location_id, layer_id)`. Dedup-lookup adds the corresponding clauses. More columns in the constraint, slightly larger index. Catches the caller-bug case.
- **(B)** Treat the caller bug as a hard error: enforce `UNIQUE (issue_id)` globally on a separate `issue_id_registry` table. Every envelope's issue_ids are inserted into this table; duplicates cause an immediate insert failure. Simpler dedup; rejects caller-bug submissions outright.

Default to (A). Test (B) in tandem; might be cleaner operationally.

### 7.5 Step 3 — Bounded snapshot hydration

```rust
fn step3_hydrate_snapshot(sorted: &SortedPoolKeys, replayed_issue_ids: &[i64]) -> SnapshotData {
    // 3a: Layer data for SKU pools, per-pool LIMIT
    let layers = spi_execute(
        "SELECT layer_id, sku_id, location_id, effective_qty, unit_cost, born_at, born_seq
         FROM (
             SELECT *, ROW_NUMBER() OVER (PARTITION BY sku_id, location_id ORDER BY born_at ASC, born_seq ASC) as rn
             FROM cost_layers
             WHERE (sku_id, location_id) IN (SELECT * FROM UNNEST($1, $2))
               AND effective_qty > 0
         ) sub
         WHERE rn <= $3",
        sorted.sku_keys,
        layer_limit_per_pool
    );

    // 3b: WAC running average for WAC/AVG pools — read from incremental state table.
    // (NOT reconstructed from posting_lines history; that would be O(history_size)
    //  per event and defeat the bulk-UNNEST throughput benefit.)
    let wac_state = spi_execute(
        "SELECT sku_id, location_id, avg_unit_cost, avg_total_qty
         FROM wac_pool_state
         WHERE (sku_id, location_id) IN (SELECT * FROM UNNEST($1, $2))",
        sorted.sku_keys
    );
    // Missing rows: pool has not yet had a receipt event. plan_apply must
    // handle this case (consumption from a never-received pool is an error;
    // a receipt to a never-received pool initializes the state row).

    // 3c: Standard costs for STD-method pools — latest effective_from per pool.
    let standard_costs = spi_execute(
        "SELECT DISTINCT ON (sku_id, location_id) sku_id, location_id, unit_cost
         FROM standard_costs
         WHERE (sku_id, location_id) IN (SELECT * FROM UNNEST($1, $2))
           AND effective_from <= NOW()
         ORDER BY sku_id, location_id, effective_from DESC",
        sorted.sku_keys
    );

    // 3d: WIP pool aggregates (no layers for WIP). Read from posting_line_inventory
    // because WIP value is the running accumulated cost of work done on the WO so
    // far. WIP pools do not maintain a wac_pool_state-style table because each WIP
    // pool's lifetime is bounded (it goes away when the WO closes) and the volume
    // is small.
    let wip_aggregates = spi_execute(
        "SELECT work_order_id, operation_id, SUM(amount SIGNED) as total_value, SUM(qty SIGNED) as total_qty
         FROM posting_lines pl JOIN posting_line_inventory pli USING (posting_line_id)
         WHERE (work_order_id, operation_id) IN (SELECT * FROM UNNEST($1, $2))
         GROUP BY work_order_id, operation_id",
        sorted.wip_keys
    );

    // 3e: Sequence seeds for born_seq, consumed_seq.
    // NOTE: GROUP BY omits keys with no matching rows; for a brand-new pool
    // with no prior layers, the result set will not contain that
    // (sku_id, location_id) pair at all. The hydration code MUST initialize
    // the per-pool sequence generator at 0 when a pool_key is absent from
    // the result set — the absence of a row means the pool has no prior
    // layers, and 0 is the correct starting sequence number. Same rule
    // applies to consumed_seq for newly-created layers in this SuperBatch.
    let sequence_seeds = spi_execute(
        "SELECT sku_id, location_id, MAX(born_seq) AS max_born_seq
           FROM cost_layers
          WHERE (sku_id, location_id) IN (SELECT * FROM UNNEST($1, $2))
          GROUP BY sku_id, location_id",
        sorted.sku_keys
    );
    let consumed_seq_seeds = spi_execute(
        "SELECT cl.sku_id, cl.location_id, MAX(cld.consumed_seq) AS max_consumed_seq
           FROM cost_layer_depletions cld JOIN cost_layers cl ON cld.layer_id = cl.layer_id
          WHERE (cl.sku_id, cl.location_id) IN (SELECT * FROM UNNEST($1, $2))
          GROUP BY cl.sku_id, cl.location_id",
        sorted.sku_keys
    );

    // 3f: Replayed rows for skipped events
    let replayed_rows = if !replayed_issue_ids.is_empty() {
        spi_execute(
            "SELECT issue_id, method_used, layer_id, qty, unit_cost FROM cost_layer_depletions WHERE issue_id = ANY($1::bigint[])
             UNION ALL
             SELECT issue_id, method_used, NULL as layer_id, qty, applied_unit_cost FROM cost_consumptions WHERE issue_id = ANY($1::bigint[])",
            replayed_issue_ids
        )
    } else {
        Vec::new()
    };

    SnapshotData { layers, wac_state, standard_costs, wip_aggregates, sequence_seeds, replayed_rows }
}
```

The `layer_limit_per_pool` (default 1000) bounds the per-pool layer read. If Step 4's plan_apply exhausts a pool's hydrated layers, a continuation fetch reads the next page:

```rust
fn continuation_fetch(pool: &SkuPoolKey, after_born_seq: i64, additional_count: u32) -> Vec<LayerView>;
```

The continuation fetch happens INSIDE the active FOR UPDATE transaction, so locks remain held. The lease (§9) must be sized to tolerate continuation fetches; deep-pool workloads may need lease tuning.

### 7.6 Step 4 — In-memory dispatch and execution

```rust
fn step4_execute(superbatch: &SuperBatch, hydrated_snapshot: SnapshotData, dedup: &DedupResult) -> ExecutionResult {
    // Sort events for deterministic replay. The first four keys come from
    // the envelope's payload (acct's chronological ordering rule). The last
    // two are tiebreakers from staging-side metadata: request_seq is the
    // staging queue's monotonic counter assigned at enqueue time; event_seq
    // is a 0-based index assigned when constructing the envelope's event
    // list. Together these guarantee a total order on events that exists
    // pre-INSERT — posting_line_id is assigned by Step 5's INSERT and cannot
    // serve as a sort key here.
    let mut events: Vec<Event> = superbatch.flatten_events();
    events.sort_unstable_by_key(|e| (e.business_date, e.doc_chrono, e.document_id, e.sub_priority, e.request_seq, e.event_seq));

    // Excluded envelopes accumulate across replay passes: any envelope whose
    // plan_apply errored is added to this set, and subsequent replays skip
    // all events from envelopes in the set. The pristine_snapshot is the
    // post-Step-3 snapshot BEFORE any plan_apply mutations; it is cloned
    // (cheap — a few hundred pool states) and used as the starting point
    // for each replay pass.
    let pristine_snapshot = hydrated_snapshot.clone();
    let mut excluded: HashSet<CorrelationId> = HashSet::new();
    let mut envelope_errors: HashMap<CorrelationId, EnvelopeError> = HashMap::new();

    loop {
        let mut working_snapshot = pristine_snapshot.clone();
        let mut depletion_rows = Vec::new();
        let mut consumption_rows = Vec::new();
        let mut layer_inserts = Vec::new();
        let mut posting_line_rows = Vec::new();
        let mut posting_line_inventory_rows = Vec::new();
        let mut posting_line_dimension_rows = Vec::new();
        let mut provisional_rows = Vec::new();
        let mut per_envelope_results: HashMap<CorrelationId, EnvelopeResult> = HashMap::new();
        let mut new_failure: Option<CorrelationId> = None;

        for event in &events {
            // Skip events from already-excluded envelopes.
            if excluded.contains(&event.correlation_id) {
                continue;
            }

            if dedup.is_replayed(event) {
                let result = compute_replay_result(event, &working_snapshot.replayed_rows);
                per_envelope_results.entry(event.correlation_id).or_insert_default().add_replay(result);
                continue;
            }

            let method = dispatch_event(event);
            match method.plan_apply(event, &working_snapshot) {
                Ok(plan_result) => {
                    depletion_rows.extend(plan_result.depletions.clone());
                    consumption_rows.extend(plan_result.consumptions.clone());
                    layer_inserts.extend(plan_result.layer_inserts.clone());
                    posting_line_rows.extend(plan_result.posting_lines.clone());
                    posting_line_inventory_rows.extend(plan_result.posting_line_inventory.clone());
                    posting_line_dimension_rows.extend(plan_result.posting_line_dimensions.clone());
                    provisional_rows.extend(plan_result.provisional_rows.clone());
                    working_snapshot.apply_plan_result(&plan_result);
                    per_envelope_results.entry(event.correlation_id).or_insert_default().add_success(plan_result);
                }
                Err(plan_err) => {
                    // Record the failure, add envelope to excluded set, restart
                    // the pass from pristine. Subsequent events (especially from
                    // OTHER envelopes that may have already applied against this
                    // envelope's now-reverted mutations) need to re-execute
                    // against the corrected snapshot.
                    envelope_errors.insert(event.correlation_id, plan_err);
                    new_failure = Some(event.correlation_id);
                    break;
                }
            }
        }

        match new_failure {
            None => {
                // Clean pass completed; partition results.
                let committed_correlation_ids: HashSet<_> = per_envelope_results.keys().copied().collect();
                // Extract the final WAC pool state from the working snapshot.
                // working_snapshot is local to the replay loop; its mutations
                // are lost when the function returns unless we extract them
                // into the result here. Step 5's UPSERT (§7.7) takes this as
                // its bind array; the array is already deduplicated by
                // (sku_id, location_id) because working_snapshot.wac_state is
                // a HashMap keyed by (sku_id, location_id) — see invariant
                // I-upsert-array-unique (§3.2).
                let wac_state_upserts: Vec<WacStateUpsert> = working_snapshot.wac_state
                    .iter()
                    .filter(|(_, state)| state.was_mutated_in_this_superbatch)
                    .map(|(key, state)| WacStateUpsert {
                        sku_id: key.0,
                        location_id: key.1,
                        avg_unit_cost: state.avg_unit_cost,
                        avg_total_qty: state.avg_total_qty,
                        last_updated_at: now(),
                        last_committer_tx_id: committer_tx_id,
                    })
                    .collect();
                return ExecutionResult {
                    depletion_rows,
                    consumption_rows,
                    layer_inserts,
                    posting_line_rows,
                    posting_line_inventory_rows,
                    posting_line_dimension_rows,
                    provisional_rows,
                    wac_state_upserts,
                    committed_results: per_envelope_results.into_iter()
                        .filter(|(c, _)| committed_correlation_ids.contains(c)).collect(),
                    failed_results: envelope_errors.into_iter().collect(),
                    excluded_envelopes: excluded,
                };
            }
            Some(failed_id) => {
                excluded.insert(failed_id);
                // Loop continues: replay from pristine, skipping all excluded.
            }
        }
    }
}
```

**Per-envelope failure isolation via pristine-snapshot replay.** A business-logic error (`InsufficientInventory`, `MethodMismatch`, etc.) on one envelope does NOT cause the SuperBatch to fail. The failed envelope's rows are excluded from Step 5's UNNEST; the surviving envelopes commit.

The replay strategy is mandatory for path-dependent costing methods (FIFO, AVG, any WAC variant). A naive per-envelope delta-rollback (mutating in place, then reverting on failure) corrupts subsequent envelopes that have already booked rows against the now-reverted intermediate state. Worked example:

1. Event 1 (Env A): Receives 10 units @ $5 into an empty AVG pool. avg = $5, qty = 10.
2. Event 2 (Env B): Receives 10 units @ $15. avg = $10, qty = 20.
3. Event 3 (Env C): Issues 5 units, booking at the current $10 average. qty = 15. Cost = $50.
4. Event 4 (Env B, different pool): plan_apply errors.
5. Delta-rollback would revert Env B's mutations on the first pool: avg → $5, qty → 10.
6. But Env C's already-emitted row says the issue happened at $10. The pool's persisted state ($5 avg, qty=15 - 5 = 5 after the now-uncoupled Env C issue) is mathematically inconsistent with the row Env C produced.

Pristine-snapshot replay handles this correctly: when Env B fails, we discard the working snapshot entirely, add Env B to the excluded set, restore from the pristine pre-Step-4 snapshot, and re-run plan_apply for all surviving events. Env C now sees avg = $5 (Env B's mutations never applied), books its issue at $5 ($25 cost), and the pool ends consistent at $5 avg with the correct qty.

**Cost.** One snapshot clone per replay pass. The snapshot is a few hundred pool states (one per (sku_id, location_id) the SuperBatch touched, plus WIP entries); cloning is O(pools_in_superbatch) which is bounded by the affinity-grouped component size. Worst case for replay passes: every envelope fails sequentially → O(envelopes_in_superbatch) passes, each doing O(events_in_superbatch) work, total O(envelopes × events). In practice business-logic failures are rare and concentrated; the typical case is 0 or 1 replay passes per SuperBatch.

**Note on snapshot ownership.** The function takes `hydrated_snapshot: SnapshotData` by value (not `&mut`), keeps the pristine clone immutable in the loop, and produces a fresh `working_snapshot` clone per pass. The trait's plan_apply signature receives `&SnapshotData` (read-only access; the function returns a plan_result whose application to the snapshot is the committer's responsibility via `working_snapshot.apply_plan_result`). This separation is what makes pristine-replay clean: plan_apply itself is pure-functional over the snapshot; mutation is explicit and visible at the call site.

**Note on WacState dirty tracking.** The `working_snapshot.wac_state` is a `HashMap<(sku_id, location_id), WacState>` carrying per-pool running averages. Each `WacState` carries a `was_mutated_in_this_superbatch: bool` flag, set true when `apply_plan_result` mutates the pool's avg_unit_cost or avg_total_qty. At clean-pass completion, the result's `wac_state_upserts` array is built only from dirty entries (filter by the flag). This ensures the Step 5 UPSERT carries one row per dirty pool — never per-event — so the array satisfies invariant I-upsert-array-unique by construction. Pools that were merely read (hydrated for snapshot context but never mutated) are excluded from the UPSERT, saving SPI bandwidth and avoiding unnecessary index updates.

The chronological event sort ensures that within one replay pass, each event's plan_apply runs against the cumulative effect of all PRIOR successful events on the same pool. Across replay passes, the excluded set monotonically grows, so the algorithm terminates.

**All-or-nothing alternative (§14 A10):** some workflows (e.g., financial postings that span multiple envelopes intentionally bundled for atomicity-by-business-rule) might want SuperBatch-level all-or-nothing semantics. The trait protocol can support this via a `must_atomic_with` field on the envelope; if any envelope in a `must_atomic_with` group fails, all envelopes in the group fail. The router would need to keep such groups in the same SuperBatch (treating them as a transactional unit for routing purposes). Currently the router doesn't model this; envelopes are independently routed. Tandem test: validate per-envelope isolation as the default; add atomic-group support if business workflows require it.

### 7.7 Step 5 — Bulk UNNEST insertion

```rust
fn step5_bulk_insert(result: &ExecutionResult) -> Result<()> {
    // posting_lines first (other tables FK on it)
    spi_execute(
        "INSERT INTO posting_lines (posting_line_id, business_date, doc_chrono, document_id, ...)
         SELECT * FROM UNNEST($1::bigint[], $2::date[], $3::bigint[], $4::bigint[], ...)",
        result.posting_line_rows
    );

    // Layer inserts (for new layers from receipts, adjustments, etc.)
    spi_execute(
        "INSERT INTO cost_layers (layer_id, sku_id, location_id, qty, unit_cost, born_at, born_seq, correlation_id, user_tx_xid, committer_tx_id, superbatch_id)
         SELECT * FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::timestamptz[], $7::bigint[], $8::uuid[], $9::xid8[], $10::bigint[], $11::bigint[])",
        result.layer_inserts
    );

    // Depletions
    spi_execute(
        "INSERT INTO cost_layer_depletions (depletion_id, layer_id, qty, unit_cost, consumed_at, consumed_seq, issue_id, method_used, correlation_id, user_tx_xid, committer_tx_id, superbatch_id)
         SELECT * FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::timestamptz[], $6::bigint[], $7::bigint[], $8::text[], $9::uuid[], $10::xid8[], $11::bigint[], $12::bigint[])",
        result.depletion_rows
    );

    // Consumptions
    spi_execute(
        "INSERT INTO cost_consumptions ...",
        result.consumption_rows
    );

    // Posting line ancillaries
    spi_execute("INSERT INTO posting_line_inventory ...", result.posting_line_inventory_rows);
    spi_execute("INSERT INTO posting_line_dimensions ...", result.posting_line_dimension_rows);
    spi_execute("INSERT INTO posting_lines_provisional ...", result.provisional_rows);

    // WAC running-average state UPSERT (only for AVG-method pools touched by this batch).
    // plan_apply computed the new running average in-memory; this UPSERT persists it.
    // The lex-lock on pool_locks ensures no concurrent committer modifies the same pool.
    //
    // CRITICAL: the UNNEST input arrays MUST contain exactly one entry per
    // (sku_id, location_id) — the FINAL cumulative state after ALL events in
    // the SuperBatch have applied to that pool's in-memory snapshot. If the
    // committer's array builder appends snapshot state per-event, a pool touched
    // by N events ends up in the array N times, and PG aborts the transaction
    // with `ERROR: ON CONFLICT DO UPDATE command cannot affect row a second time`.
    // The committer must collect the FINAL snapshot state for each pool keyed by
    // (sku_id, location_id) — typically via a HashMap<(i64, i64), WacState>
    // populated during Step 4 — and emit one UNNEST row per distinct key.
    // See invariant I-upsert-array-unique (§3.2).
    spi_execute(
        "INSERT INTO wac_pool_state (sku_id, location_id, avg_unit_cost, avg_total_qty, last_updated_at, last_committer_tx_id)
         SELECT * FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::timestamptz[], $6::bigint[])
         ON CONFLICT (sku_id, location_id) DO UPDATE SET
           avg_unit_cost = EXCLUDED.avg_unit_cost,
           avg_total_qty = EXCLUDED.avg_total_qty,
           last_updated_at = EXCLUDED.last_updated_at,
           last_committer_tx_id = EXCLUDED.last_committer_tx_id",
        result.wac_state_upserts  // already deduplicated by (sku_id, location_id)
    );

    // Persistent staging transition staged → completed (only when SuperBatch
    // contains at least one durable_queue=true envelope). Bundled into the
    // bulk-write phase rather than per-envelope on the claim path; the in_shmem
    // state is reserved for the recovery sweep diagnostic, NOT visited on the
    // hot path.
    if result.has_durable_envelopes() {
        spi_execute(
            "UPDATE ledger_persistent_staging
             SET state='completed'
             WHERE correlation_id = ANY($1::uuid[])",
            result.durable_correlation_ids()
        );
    }

    // Status writes (UPSERT — required for committer_lazy mode correctness).
    // Each transition uses INSERT ... ON CONFLICT (correlation_id) DO UPDATE
    // rather than plain UPDATE. Under `committer_lazy`, the caller's enqueue
    // does NOT INSERT the initial 'queued' row; a plain UPDATE would match
    // zero rows and silently lose the terminal state, leaving pollers hanging
    // indefinitely. Under `caller_intx`, the row already exists at enqueue
    // time and the UPSERT's ON CONFLICT branch fires; behavior is identical
    // to plain UPDATE at microsecond cost. Universal UPSERT keeps the code
    // path single regardless of mode.
    //
    // The INSERT path needs to populate the NOT NULL enqueued_at column;
    // pass it as an additional UNNEST array from staging.enqueued_at_micros.
    // ON CONFLICT preserves the existing enqueued_at — the value bound for
    // the INSERT path only matters for committer_lazy mode where the row is
    // being created here for the first time.
    spi_execute(
        "INSERT INTO ledger_submission_status (correlation_id, state, enqueued_at, committed_at, committer_tx_id, superbatch_id)
         SELECT u.id, 'committed', u.enqueued_at, now(), $1, $2
           FROM UNNEST($3::uuid[], $4::timestamptz[]) AS u(id, enqueued_at)
         ON CONFLICT (correlation_id) DO UPDATE SET
           state = EXCLUDED.state,
           committed_at = EXCLUDED.committed_at,
           committer_tx_id = EXCLUDED.committer_tx_id,
           superbatch_id = EXCLUDED.superbatch_id",
        committer_tx_id, superbatch_id,
        result.committed_correlation_ids(),
        result.committed_enqueued_ats()
    );

    spi_execute(
        "INSERT INTO ledger_submission_status (correlation_id, state, enqueued_at, processed_at, error_code, error_detail)
         SELECT u.id, 'failed', u.enqueued_at, now(), u.err_code, u.err_detail
           FROM UNNEST($1::uuid[], $2::timestamptz[], $3::text[], $4::jsonb[]) AS u(id, enqueued_at, err_code, err_detail)
         ON CONFLICT (correlation_id) DO UPDATE SET
           state = EXCLUDED.state,
           processed_at = EXCLUDED.processed_at,
           error_code = EXCLUDED.error_code,
           error_detail = EXCLUDED.error_detail",
        /* per-envelope arrays */
    );

    spi_execute(
        "INSERT INTO ledger_submission_status (correlation_id, state, enqueued_at, processed_at, committer_tx_id)
         SELECT u.id, 'replayed', u.enqueued_at, now(), $1
           FROM UNNEST($2::uuid[], $3::timestamptz[]) AS u(id, enqueued_at)
         ON CONFLICT (correlation_id) DO UPDATE SET
           state = EXCLUDED.state,
           processed_at = EXCLUDED.processed_at,
           committer_tx_id = EXCLUDED.committer_tx_id",
        committer_tx_id,
        result.replayed_correlation_ids(),
        result.replayed_enqueued_ats()
    );

    // Webhook delivery rows for all terminal states
    spi_execute(
        "INSERT INTO webhook_deliveries (correlation_id, payload, target_url, state, next_attempt_at) SELECT * FROM UNNEST(...)",
        result.webhook_delivery_rows()
    );

    CommitTransactionCommand();
    Ok(())
}
```

The transaction commit at the end releases all FOR UPDATE locks atomically. The commit fsync is one operation per SuperBatch — the WAL throughput benefit v2.1 promises.

### 7.8 Post-commit cleanup

After successful Step 5 commit:

1. Mark the committer queue entry `in_flight → completed (3)`.
2. Mark all the SuperBatch's staging entries `routed → empty (0)`. The slots are now reusable.
3. Signal the staging queue's backpressure condition variable to wake any blocked enqueue calls.
4. The webhook delivery worker (see §12) picks up the freshly-inserted webhook_deliveries rows on its next tick.

### 7.9 Transaction failure handling

If Step 5's INSERTs fail (deadlock with non-extension code, FK violation, constraint violation that wasn't caught by dedup-lookup):

- AbortCurrentTransaction. The transaction rolls back; no rows persist (no fsync happened).
- All envelopes in the SuperBatch get state = 'failed' with error_code reflecting the failure class. Status writes happen in a fresh top-level transaction immediately following the failed one.
- Locks release on rollback.
- Webhook deliveries are inserted for the failed envelopes (in the same fresh status-write transaction).
- Committer queue entry is marked completed.

If the dedup-lookup's UNIQUE constraint is what triggers the failure (a bug in dedup-lookup logic let a duplicate through), this is a hard bug, not a normal path. Log it loudly; treat as a class-level failure of dedup.

---

## 8. Trait Dispatch

### 8.1 Inherited from design-v2

The trait protocol is unchanged from design-v2 (§3 of that doc):

```rust
pub trait CostingMethod: Send + Sync + 'static {
    const METHOD_ID: &'static str;
    type Snapshot: PoolSnapshot;

    fn required_dimensions() -> &'static [DimensionKind];
    fn plan_apply(&self, batch: &PostingApplyRequest, snapshot: &Self::Snapshot) -> ApplyResult;
    fn validate_invariants(&self, plan: &PlanResult, snapshot: &Self::Snapshot) -> Result<()>;

    // Optional methods (pay-as-you-go):
    fn close_hook_preprocess_stream(...) { ... }
    fn close_hook_participates(...) { false }
    fn emit_variance(...) { VarianceEmission::NoEmission }
    fn replay_event(...) { ... }
    fn snapshot_extra_reads(...) -> Vec<TableRef> { Vec::new() }
    fn lifecycle_hook(...) { ... }
}
```

FIFO, specific-id, standard cost implement the trait directly. WAC family (perpetual, periodic, retroactive) is in the plpgsql `cost_method_strategies` registry, dispatched via a trait adapter that bridges to plpgsql.

### 8.2 Event-type → method dispatch

The committer's Step 4 dispatch table maps `event_type_id` (the discriminator on the envelope) to the trait method instance:

```rust
fn dispatch_event(event: &Event) -> &'static dyn CostingMethod {
    match event.event_type_id {
        EVENT_WO_COMPLETE => /* lookup sku → method */ ,
        EVENT_PO_RECEIPT => /* receipt method based on receiving SKU */,
        EVENT_SO_SHIPMENT => /* method based on shipped SKU's cost_method assignment */,
        EVENT_INV_TRANSFER => /* transfer method (special) */,
        // ...
    }
}
```

For most event types, the method is determined by looking up the SKU's `cost_method` assignment in `sku_method_assignments` (cached per-committer per-snapshot for the duration of a SuperBatch). For SKU-less events (e.g., a manual journal entry crediting a WIP account), the method is determined by the credit-side account's class and the WO's configured method.

### 8.3 Snapshot construction per method

Each method's `Self::Snapshot` is constructed from the SnapshotData hydrated in Step 3. FIFO's snapshot contains the layer Vec (read via `ROW_NUMBER() OVER (PARTITION BY pool_key) <= layer_limit_per_pool` from `cost_layers`); WAC perpetual's snapshot contains the aggregate value and qty (read from a dedicated incremental-state table — see below); standard cost's snapshot contains the standard_cost lookup result (read with `DISTINCT ON (pool_key) ... ORDER BY effective_from DESC` from `standard_costs`). The committer's Step 3 hydration is generic; the snapshot construction is method-specific and happens at Step 4 dispatch time.

**Incremental-state pattern for WAC/AVG methods.** The running average for a WAC perpetual pool is NOT reconstructed from `posting_lines` history on every event. That would be O(history_size) per event and defeat the bulk-UNNEST throughput benefit. Instead, the running average is maintained incrementally in a dedicated state table:

```sql
CREATE TABLE wac_pool_state (
    sku_id               BIGINT NOT NULL,
    location_id          BIGINT NOT NULL,
    avg_unit_cost        BIGINT NOT NULL,      -- running average
    avg_total_qty        BIGINT NOT NULL,      -- signed pool depth
    last_updated_at      TIMESTAMPTZ NOT NULL,
    last_committer_tx_id BIGINT NOT NULL,
    PRIMARY KEY (sku_id, location_id)
);
```

The receipt formula for the running average:

```
new_total_value = (avg_unit_cost × avg_total_qty) + (receipt_qty × receipt_unit_cost)
new_total_qty   = avg_total_qty + receipt_qty
IF new_total_qty == 0:
    new_avg_unit_cost = avg_unit_cost   (preserve prior average; pool is empty,
                                          average is undefined but kept stamped
                                          on the pool so the next receipt has
                                          a sane starting point)
ELSE:
    new_avg_unit_cost = new_total_value / new_total_qty   (with appropriate rounding)
```

The `new_total_qty == 0` edge case arises when a receipt exactly offsets a prior negative pool balance. The mathematically-undefined "average over zero items" is handled by preserving the prior average; the next receipt against a zero-depth pool naturally resets the average to that receipt's unit cost. The implementation MUST guard this case explicitly; a literal divide would panic. Similar guards apply to `receipt_qty == 0` (caller bug — reject with `error_code='ZeroQtyReceipt'`) and to `WoComplete` outputs where `output_qty == 0` (`error_code='ZeroQtyOutput'`).

Step 3 reads the current state with a single SELECT. Step 5 UPSERTs the new state alongside the cost rows in the same top-level transaction:

```sql
INSERT INTO wac_pool_state (sku_id, location_id, avg_unit_cost, avg_total_qty, last_updated_at, last_committer_tx_id)
SELECT * FROM UNNEST(...)
ON CONFLICT (sku_id, location_id) DO UPDATE SET
  avg_unit_cost = EXCLUDED.avg_unit_cost,
  avg_total_qty = EXCLUDED.avg_total_qty,
  last_updated_at = EXCLUDED.last_updated_at,
  last_committer_tx_id = EXCLUDED.last_committer_tx_id;
```

The lex-lock on `pool_locks` ensures no concurrent committer modifies the same pool's average, so this read-mutate-write cycle is race-free. The state row is created lazily on a pool's first receipt event (the receipt establishes the initial average). For lot-tracked SKUs, a corresponding `wac_pool_state_lot` table keyed by `(sku_id, location_id, lot_id)` is used.

**CRITICAL: the UNNEST input arrays MUST contain exactly one entry per (sku_id, location_id)** — the FINAL cumulative state after ALL events in the SuperBatch have applied. If the committer's array builder appends snapshot state per-event, a pool touched by N events ends up in the array N times, and PG aborts the SuperBatch with `ERROR: ON CONFLICT DO UPDATE command cannot affect row a second time`. See invariant I-upsert-array-unique (§3.2).

Other WAC variants (periodic, retroactive) inherit the same pattern with their own state tables tailored to their semantics.

The `required_dimensions()` declaration on the trait tells the framework which posting_line_dimensions to read into the snapshot. This is a per-method tag, not a per-event one.

### 8.4 Multi-method SuperBatches

A SuperBatch can mix events using different methods (e.g., FIFO for Item A and WAC for Item B, both consumed by the same WO). The dispatcher routes each event to its method; the in-memory snapshot is built once for the SuperBatch (covering all pools) and method-specific views are constructed lazily.

The lex-lock acquisition in Step 2 is method-agnostic; it locks pools regardless of which method consumes them.

---

## 9. Failure and Recovery

### 9.1 Committer death

If a committer dies (SIGKILL, OOM, panic) between Step 2 lock acquisition and Step 5 commit, PG releases the FOR UPDATE locks automatically on backend exit. The committer queue entry remains in state `in_flight (2)` with the dead committer's stored (committer_slot, committer_token).

Recovery is opportunistic: any active committer that pulls a fresh SuperBatch first checks whether existing in-flight entries have stale leases (`now_ns - committer_acquired_at_ns > lease_ms`) AND the stored (slot, token) no longer identifies a live worker via the CommitterIdentityRegistry lookup (§5.5). If both, the recovering committer CAS-claims the entry, replacing the stale (slot, token) with its own and setting committer_acquired_at_ns to now.

**No heartbeat needed.** A legitimately-slow but alive committer is NOT subject to false takeover because the registry-based (slot, token) check protects it. The lease alone is not sufficient grounds for takeover; the registry liveness check is the authoritative test. This is why the design does not include a "touch the lease" / heartbeat mechanism — a slow committer doing legitimate work (e.g., a continuation fetch on a deep pool) does not need to extend its lease; the contending committer sees the registry slot still active with matching token and backs off. The lease is a hint to investigate; the (slot, token) check is the verdict. Without this, false takeovers would race with legitimately-slow committers and dedup-lookup would be the only thing preventing double-execution — workable but wasteful. The (slot, token) check makes false takeover essentially impossible AND is safe against OS PID recycling in containerized environments.

After CAS, the recovering committer:

1. Checks pg_xact for the dead committer's committer_tx_id (stored on the queue entry). Because each SuperBatch ran in its own top-level transaction, the XID's status is unambiguous:
   - `committed`: the work is durable. The committer reads the cost rows by superbatch_id, marks the staging entries empty, fires webhooks for committed envelopes (idempotent — webhook_deliveries has its own state machine).
   - `aborted`: the work is gone. The committer marks the queue entry back to `ready (1)` for re-processing. The dedup-lookup at next attempt will produce zero hits (since the aborted committer's INSERTs didn't persist), so reprocessing happens cleanly.
   - `in_progress`: this should not happen if the dead committer's process is genuinely gone (postmaster reaps the in-flight tx). If observed, treat as `aborted` after a short poll bound; log loudly as a possible pg_xact lag or in-flight zombie.
2. If the dead committer's tx aborted: re-execute the full pipeline (Steps 1-5). Dedup-lookup at Step 2.5 is critical — it ensures any partially-committed work (none expected, but defensive) is detected.

### 9.2 Caller user-tx rollback

Under Option (C) coupling (§4.4 default), the caller's user_tx_xid is stamped on the envelope. Before processing, the committer checks pg_xact:

```rust
fn check_caller_tx_status(user_tx_xid: pg_sys::FullTransactionId) -> CallerTxStatus {
    let status = pg_xact_status(user_tx_xid);  // SPI or direct C call
    match status {
        Committed => CallerTxStatus::OK,
        Aborted => CallerTxStatus::Aborted,
        InProgress => CallerTxStatus::Eject,
    }
}
```

If the caller's user-tx aborted, the envelope is dropped — set state = 'failed' with error_code = 'caller_tx_aborted', fire webhook, release staging slot. No cost rows written.

**If still in progress, the committer EJECTS the envelope; it never sleeps.** Per §4.4, the committer increments the staging entry's eject_count, CAS-flips the staging entry from `routed (3)` back to `pending (1)`, and continues with the rest of the SuperBatch. The router re-picks the ejected envelope on its next tick. The cycle terminates when the caller's user-tx eventually transitions to committed or aborted, OR when the wall-clock `caller_tx_timeout_ms` is reached, OR when the safety bound `max_eject_count` is reached.

The wall-clock `caller_tx_timeout_ms` is the primary bound (PoC default 30s, production deployments may adjust based on workload tolerance). `max_eject_count` (default 10000) is the defensive safety bound for pathological cycling; under normal operation the wall-clock fires first. See §4.4 for the full mechanism and rationale on why committers must never sleep on in_progress caller txs.

### 9.3 Partial-batch failure

Per §7.6, a business-logic error on one envelope does NOT fail the SuperBatch. The failed envelope's rows are excluded from Step 5's INSERT; other envelopes commit. Failed envelopes get state = 'failed' with the specific error_code.

The committer's Step 5 INSERTs run as one top-level transaction. If the transaction itself fails (deadlock, FK violation, etc.), ALL envelopes in the SuperBatch get state = 'failed' with error_code = 'committer_tx_failure'. The locks release; the committer queue entry is marked completed; webhooks fire for all failed envelopes.

### 9.4 Postmaster restart

Postmaster restart loses all shmem state. Recovery on `_PG_init` runs in two phases before queues accept new traffic.

**Phase 1: Replay durable envelopes (if `ledger.persistent_staging = on`).**

The startup recovery worker scans the persistent staging table:

```sql
SELECT correlation_id, user_tx_xid, event_type, payload, sku_pool_keys,
       wip_pool_keys, business_date
FROM ledger_persistent_staging
WHERE state IN ('staged', 'in_shmem')
ORDER BY request_seq;
```

For each row:

- Check if cost rows already exist for this `correlation_id` (`SELECT 1 FROM posting_lines WHERE correlation_id = $1 LIMIT 1`). If yes: the committer committed before the crash. UPDATE `ledger_persistent_staging.state = 'completed'`; ensure `ledger_submission_status` reflects `committed`. Move on.
- Otherwise check `pg_xact_status(user_tx_xid)`:
  - `committed`: caller's user-tx committed durably. Re-INSERT the envelope into the shmem staging queue with CAS `valid: 0 → 1 (pending)`. Set `ledger_persistent_staging.state = 'in_shmem'`. The router will pick it up on its next tick and processing resumes normally.
  - `aborted`: caller rolled back; the persistent_staging row is orphaned. DELETE the row. Under `caller_intx`, no submission_status row exists (rolled back with caller's user-tx); no action needed. Under `committer_lazy + persistent_staging`, INSERT a `state='failed', error_code='caller_tx_aborted'` row directly via `ON CONFLICT (correlation_id) DO NOTHING`. Fire failure webhook.
  - `in_progress`: defensive — shouldn't happen after postmaster restart (all backends killed, all txs definitionally aborted in pg_xact). Treat as aborted.
  - **NULL return OR error from pg_xact_status:** the XID is past PG's CLOG truncation horizon. This can occur after extended downtime, after the extension was paused while other database activity advanced the freeze horizon, or in any environment where persistent_staging rows survived long enough for their user_tx_xid to be frozen out of pg_xact. The safe semantic: treat the row as abandoned. Under PG 13+, `pg_xact_status` returns NULL for truncated XIDs; under older versions it may raise an error matching SQLSTATE class 22 or 25 — the recovery worker must catch either case. Action: DELETE the persistent_staging row; under `committer_lazy + persistent_staging`, INSERT a `state='failed', error_code='caller_tx_abandoned'` row. Log a WARNING containing the correlation_id, user_tx_xid, and persistent_staging.enqueued_at so operators can audit if the abandonment cohort is unexpected. A well-tuned deployment should never see this case; its presence indicates either an extension outage longer than `vacuum_freeze_table_age` worth of XID consumption, or a configuration anomaly.

**Phase 2: Sweep submission_status for non-durable envelopes.**

```sql
SELECT correlation_id FROM ledger_submission_status
WHERE state IN ('queued', 'processing')
  AND correlation_id NOT IN (SELECT correlation_id FROM ledger_persistent_staging);
```

These are envelopes that were submitted with `durable_queue=false`. For each:

- Check cost rows by correlation_id.
  - If rows exist: the committer committed before the crash. UPDATE state to `committed` (or `replayed` depending on attribution). Fire missed webhooks.
  - If no rows: the envelope was lost when shmem was reset. UPDATE state to `failed` with `error_code='postmaster_restart_loss'`. Fire failure webhook.

Note: if `ledger.persistent_staging=off`, Phase 1 is skipped entirely and all in-flight envelopes from Phase 2 with no cost rows are lost (marked failed). This is the documented behavior for deployments choosing the non-durable path.

**Phase 3: Resume normal operation.**

- The webhook delivery worker resumes processing `webhook_deliveries` rows.
- Per-pool `next_born_seq` and `next_consumed_seq` counters are seeded lazily on first committer access per pool (not eagerly at startup; per §7 hydration).
- Router and committers resume normal duty.

**Durability guarantee summary:**

- `durable_queue=true` + `persistent_staging=on`: envelope survives postmaster restart. The caller observes a brief delay (the recovery sweep duration) before processing resumes, but the work eventually happens.
- `durable_queue=false` (any deployment): envelope is lost if postmaster restarts before the committer commits. Recovery marks it `failed`; caller must re-submit.

Recovery time scales linearly with in-flight persistent_staging row count. Bounded by the GC retention window × peak durable submission rate.

### 9.5 Slot exhaustion / leak

Slots in the staging and committer queues can leak if state transitions are incomplete. The recovery sweep (router boot) cleans up `processing` entries with no committer queue link.

A periodic audit (every 60s) scans both queues for:
- Staging entries in state `processing` for > 60s with `pg_pid_alive(backend_pid) = false`: revert to `pending`. (Backend_pid here refers to the originating caller's backend, distinct from committer identity; caller backend liveness is the standard PG check.)
- Staging entries in state `routed` for > 5min with no corresponding committer queue entry: mark as `failed` (with error_code = 'committer_lost') and free.
- Staging entries in state `routed` for > 5min linked to a CommitterQueueEntry in state `completed (3)` whose stored (committer_slot, committer_token) no longer identifies a live worker (via the CommitterIdentityRegistry lookup, §5.5): reap the staging entries directly (CAS routed → empty, free each staging entry's arena blocks); free the queue entry's OWN arena blocks (staging_entry_offsets array, sorted pool_keys arrays — these are queue-entry-owned, not staging-entry-owned, and must be freed separately); CAS queue entry completed → empty. Cost rows are durable; this is cleanup of post-commit-pre-cleanup deaths. This audit rule converges with §6.4's router-sweep handling for the same case (defense in depth).
- Committer queue entries in state `in_flight` for > 5min whose stored (committer_slot, committer_token) no longer identifies a live worker: orphan-recover per §9.1.

### 9.6 The "eventual resolution" invariant (I-eventual-resolution)

Every envelope eventually reaches a terminal state in `ledger_submission_status` (committed | failed | replayed) within `MAX_RESOLUTION_BOUND = max(committer_lease_ms × 10, queue_full_timeout_ms × 2, 5 minutes)`. The bound is constructed to cover:
- Normal committer processing (1-10× lease at worst under contention).
- Backpressure waits (callers may have been blocked; once unblocked, processing proceeds normally).
- Recovery sweep cycles (60s periodic audit).
- Webhook delivery (handled separately; doesn't gate ledger_submission_status terminal state).

Property-test invariant (carried from PoC spec): for any sequence of failure injections, every envelope eventually transitions to a terminal state.

---

## 10. Close Hook Integration

### 10.1 The drain-to-zero requirement

A period's close hook (closing fiscal period N) operates on the cost data for transactions with `business_date ≤ N.end_date`. v2.1's async execution model means there's a window where the caller has submitted a transaction (business_date in period N) but the committer hasn't yet committed the cost rows.

If the close hook fires during this window, it sees an incomplete period. The WAC corrections it computes will be wrong (missing the not-yet-committed event's impact).

The drain-to-zero requirement: before the close hook for period N starts, the system must drain all envelopes with business_date in period N or earlier. Formally:

```
∀ envelope e in (staging_queue ∪ committer_queue):
    e.business_date > N.end_date
```

The close hook coordinator queries:

```sql
SELECT COUNT(*) FROM ledger_submission_status
WHERE state IN ('queued', 'processing')
  AND business_date <= $period_end_date
```

The partial index `ledger_submission_status_state` on `(state) WHERE state IN ('queued','processing')` makes this query fast: the planner bitmap-scans all in-flight rows (a bounded set — a few thousand rows at most under normal operation; backpressure caps it) and filters by `business_date` in memory. No separate `business_date` partial index is needed; adding one would double the index-churn cost on every router and committer state transition without meaningfully improving this low-frequency administrative query (see §1.1 indexing rationale).

If the count is > 0, the close hook waits. Polling interval is `close_hook_drain_poll_ms` (default 100ms); timeout is `close_hook_drain_timeout_ms` (default 30 minutes).

If the timeout elapses with non-zero count, the close hook fails with an operational error: "unable to drain pending submissions in time; check for stuck envelopes." This requires human intervention.

### 10.2 Backpressure during close

Once the drain-to-zero completes for period N, the close hook begins. During the close hook's execution, can new envelopes for period N+1 be enqueued?

- **Lenient**: yes. The close hook reads period N's cost rows; new envelopes for N+1 don't affect that read. The committer continues processing N+1 envelopes during close.
- **Strict**: no. The system enters "closing mode" and rejects enqueues until close completes.

Lenient is the right default. The committer keeps draining N+1 envelopes; the close hook's pool iteration in Kahn order reads only committed rows for period N. There's no race because the close hook doesn't touch N+1 data.

Exception: if the close hook needs to write back-dated corrections to period N (the typical WAC retroactive case), those corrections target period N's cost rows. The close hook acquires its own pool_locks FOR UPDATE for the pools it's correcting. Concurrent committers processing N+1 envelopes for the same pools wait. The lock ordering must be respected (the close hook is itself a sole-writer-style operation during this phase).

**Flagged concern (§14 A13):** the close hook can correct period N while the committer commits period N+1 — both touching the same pool. Lex-locking serializes them safely, but the close hook's pool iteration order may conflict with the committer's per-SuperBatch sort order. If they touch the same pools, they must use the same lock ordering. Either:

- (A) Close hook acquires all locks for period N's correction phase upfront, in lex order, before any per-pool work. Heavy locks; could starve concurrent committer work.
- (B) Close hook processes pools sequentially, each in its own short transaction with its own lock acquisition. Concurrent committers acquire interleaved. Risk: a long-running pool correction in close hook holds its lock, blocking a committer SuperBatch that needs the same pool — but only that pool, not the whole batch.

Test (B); fall back to (A) if needed.

### 10.3 The close hook DAG itself

Unchanged from design-v2 §5.4. The DAG iterates pools in Kahn order (topological sort by participating_reasons edges); for each pool, walks the merged value/qty event stream chronologically; emits variance posting_lines per the four routing patterns (internal-chain, leaf-single-leg, leaf-two-leg-wash, mixed-parent-component).

v2.1 changes nothing about close hook semantics. It only changes when the close hook can safely start (drain-to-zero) and how it acquires locks (same lex-lock pattern as committer).

---

## 11. Currency, Dimensions, Lots, Units, BOM

### 11.1 Currency model: single-currency-per-transaction

**Pool identity does NOT include currency.** A SKU at a location has exactly one pool, regardless of how many currencies the broader system tracks. The pool's cost computations occur in a single currency: the subsidiary's base currency.

**The envelope carries a pinned currency.** Every envelope submitted to `ledger_enqueue` is associated with a single currency (the issuing subsidiary's base currency at the time of submission). This currency is stored as metadata on the envelope and propagated to posting_lines, but it does NOT participate in pool identity, lock acquisition, snapshot hydration, or cost computation.

**Cross-currency adjustments happen upstream.** When source business events involve a different currency (e.g., a purchase order denominated in EUR for a USD-base subsidiary), acct's FX conversion layer translates the amounts to the subsidiary's base currency BEFORE the envelope is constructed. The ledger extension never sees the original foreign-currency amounts; it sees only the converted base-currency values. FX rate tables (`fx_rates`, etc.) are NOT consulted by the extension; that's acct's concern.

**No per-currency WAC computation.** Each pool has one running average, one set of FIFO layers, one standard cost. Currency is opaque metadata, not a cost-axis.

**Consequences for the architecture:**
- Pool identity is `(sku_id, location_id)` only.
- Lock domains are `(sku_id, location_id)` for SKU pools and `(work_order_id, operation_id)` for WIP pools. No currency column anywhere.
- A 15-component WO has 15+1+1 = 17 pool_keys, period. No multiplication by currency count.
- The committer's snapshot hydration reads pool state once per pool, not once per (pool, currency) tuple.
- WAC retroactive corrections work in a single dimension; no FX-shift sensitivity in cost computation.

**Inter-subsidiary transactions:**
Transactions that genuinely span subsidiaries with different base currencies are modeled as TWO envelopes — one per subsidiary, each pinned to its own base currency. The application tier (acct's inter-company posting layer) coordinates the pair. The ledger extension treats them as independent submissions; atomicity across the pair is the application tier's responsibility, not the extension's.

**Schema consequence:**
The posting_lines table carries a `currency CHAR(3)` column directly (the envelope's pinned currency), not a separate posting_line_currencies expansion table. acct's existing schema may have `posting_line_currencies` for legacy multi-currency expansion; under v2.1's model, that table is unused (or repurposed) for envelopes submitted through the extension.

### 11.2 WAC running-average state representation

For weighted-average-cost methods (WAC perpetual, AVG-class methods), each pool maintains a running average that is updated on every receipt and read on every consumption. The committer needs an authoritative source for this average that can be loaded at snapshot hydration time (Step 3 of §7).

**Two implementation strategies:**

- **(A) Reconstruct from full history.** At Step 3, query `SELECT SUM(qty), SUM(qty * unit_cost) FROM cost_layers WHERE pool = $pool` to derive the average. O(history_size) per pool per SuperBatch. Acceptable for small pools, catastrophic for long-lived pools with millions of historical rows. Also incorrect if any signed adjustments (negative qty, scrap, etc.) exist — must aggregate from `posting_line_inventory` rather than `cost_layers`, doubling the SPI count.

- **(B) Maintain an incremental state table.** A dedicated `wac_pool_state` table holds one row per WAC-method pool: `(sku_id, location_id, avg_unit_cost, total_qty, last_updated_at, last_committer_tx_id)`. Step 3 reads via simple PK lookup: O(touched_pools) regardless of history. Step 5 UPSERTs the new average computed by `plan_apply` (in-memory). The lex-lock on `pool_locks` ensures no concurrent committer modifies the same pool, so the running-average update is race-free without additional locking on the state table.

**v2.1 adopts (B).** This is how production WAC perpetual actually works in mature accounting systems; reconstructing from history on every event defeats the purpose of a running aggregate.

**Schema:**

```sql
CREATE TABLE wac_pool_state (
    sku_id               BIGINT NOT NULL,
    location_id          BIGINT NOT NULL,
    avg_unit_cost        BIGINT NOT NULL,       -- running average
    total_qty            BIGINT NOT NULL,       -- signed pool depth
    last_updated_at      TIMESTAMPTZ NOT NULL,
    last_committer_tx_id BIGINT NOT NULL,
    PRIMARY KEY (sku_id, location_id)
);
```

For lot-tracked or unit-tracked WAC pools (rare but possible), the table has variants keyed by the additional identity dimensions, analogous to `pool_locks_lot` and `pool_locks_unit`.

**Lifecycle:**

- **Create or update in one statement:** Step 5's UPSERT (`INSERT INTO wac_pool_state ... ON CONFLICT (sku_id, location_id) DO UPDATE SET ...` — see §7.7 and §8.3) handles both the first-receipt creation and subsequent running-average updates natively. The committer holds FOR UPDATE on the pool's `pool_locks` row throughout, so the UPSERT is race-free against other committers. There is no separate `DO NOTHING` creation step; that would be an extra SPI call with no semantic benefit.
- **On every WAC SuperBatch commit:** the UPSERT carries the new running average for pools that `plan_apply` mutated (NOT for pools merely read but not modified — e.g., a SuperBatch that only does FIFO work on WAC-adjacent pools does not touch their state).
- **Read at Step 3:** `SELECT sku_id, location_id, avg_unit_cost, avg_total_qty FROM wac_pool_state WHERE (sku_id, location_id) IN (...)`. Returns the snapshot.

**Step 5 thus has 1 additional UNNEST INSERT/UPSERT** beyond the cost-row writes. The total per-SuperBatch write count is 6 (posting_lines, cost_layers, cost_layer_depletions, cost_consumptions, posting_line_inventory, wac_pool_state) when WAC pools are touched; 5 when only FIFO/standard pools are touched.

This is the same pattern adopted in the PoC (`poc_v21_avg_pool_state`); production WAC reuses the mechanism.

### 11.3 Dimensions

Per design-v2's dimension vocabulary:

- **Identity dimensions** (framework-bundled): Lot, Unit, CostLayer, CostBook. Each pool key carries identity dimension values when in scope. A lot-tracked SKU's pool is `(sku_id, location_id, lot_id)` — three columns; pool_locks must be keyed this way for lot-tracked SKUs.

- **Analytical dimensions** (EAV-extensible): routing_op, project, custom. Stored on `posting_line_dimensions`. The trait method declares which it needs via `required_dimensions()`; the framework reads them into the snapshot.

The pool_locks table needs variants for the additional identity dimensions:

```sql
CREATE TABLE pool_locks_lot (
    sku_id BIGINT NOT NULL,
    location_id BIGINT NOT NULL,
    lot_id BIGINT NOT NULL,
    lock_version BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (sku_id, location_id, lot_id)
);

CREATE TABLE pool_locks_unit (
    sku_id BIGINT NOT NULL,
    location_id BIGINT NOT NULL,
    unit_id BIGINT NOT NULL,
    lock_version BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (sku_id, location_id, unit_id)
);
```

The lex-lock acquisition becomes a sequence of queries, one per pool_locks variant, in fixed table order:

1. pool_locks (no identity dimensions)
2. pool_locks_lot
3. pool_locks_unit
4. wip_pool_locks
5. ... (other identity-dimension-extended variants if added)

Each pool_locks variant is its own lock domain; acquisition order across domains is fixed. Within each domain, lex-sort by the domain's natural key.

**Granularity constraint (load-bearing).** A given SKU's tracking granularity is FIXED — it is determined by the SKU's `sku_method_assignments` row (per R4), which assigns each (sku, location) pair to a single (method, granularity) tuple at SKU/location registration time and does not change per-event. The granularity choices are mutually exclusive: lot-tracked, unit-tracked, or base (no identity-dimension extension). A SKU configured for lot tracking emits ONLY lot-keyed events whose lock acquisition targets `pool_locks_lot`; a SKU configured for unit tracking emits ONLY unit-keyed events targeting `pool_locks_unit`; a base SKU emits ONLY events targeting `pool_locks`.

This is load-bearing because pool_locks variants are SEPARATE TABLES. If a SKU were permitted to emit, say, a lot-keyed event AND a non-lot event in different envelopes, the first would lock a row in `pool_locks_lot` and the second in `pool_locks`. The two committers would not block each other on the conceptually-same SKU pool. The sole-writer invariant (§3.2 I-sole-writer) would be violated for any shared aggregate state (e.g., location-level summaries, or a SKU's `wac_pool_state` row which is keyed only by `(sku_id, location_id)` regardless of lot — the lot-keyed committer and the non-lot committer would both attempt to read-mutate-write that row concurrently with no shared lock). Data corruption follows.

The enforcement points:

1. **Schema:** `sku_method_assignments` carries a `tracking_granularity` column (enum: `base`, `lot`, `unit`); the column is NOT NULL and is set at row creation.
2. **Enqueue validation:** `ledger_enqueue` reads the SKU's granularity from `sku_method_assignments` and rejects envelopes whose event payloads don't match (e.g., a lot-tracked SKU receiving an event without a lot_id, or a base SKU receiving a lot-keyed event). Error code: `tracking_granularity_violation`. The validation is per-event, performed in the caller's user-tx before the shmem write.
3. **Migration constraint:** changing a SKU's tracking granularity requires draining all in-flight envelopes for that SKU, running the close hook through the period boundary, then issuing an explicit `ledger_change_sku_granularity` migration function. The function locks both old- and new-domain `pool_locks` rows, verifies no in-flight events, and atomically updates `sku_method_assignments`. This is operationally heavy by design — the constraint exists to make accidental granularity drift impossible.

A future relaxation that allowed per-event granularity would require either (a) UNION-ing all pool_locks variants into one acquisition step (operationally painful, doubles SPI count), or (b) a shared "supremum" lock per (sku_id, location_id) acquired before any variant — also doubling SPI cost and adding a second lock domain. Neither is justified by current workloads. The fixed-granularity constraint stays.

**Flagged concern (§14 A15):** five lock domains is a lot. Each adds an SPI call to Step 2. Alternative: a single `pool_locks` table with all identity dimension columns and NULL placeholders for not-applicable. Cleaner schema; SQL handles NULL in ORDER BY and PRIMARY KEY with care. The trade-off: NULL semantics in keys can surprise; explicit per-domain tables are more obvious. Test both.

### 11.4 Lots

A lot-tracked SKU's events carry lot_id in the payload. The pool key includes lot_id; the lock acquisition uses pool_locks_lot.

Cost methods see lot_id as part of the pool identity. FIFO's layers are scoped per lot; the snapshot for a lot pool only includes that lot's layers.

Receiving a new lot creates new layers under that lot's pool. Consuming from a lot consumes from that lot's layers. Cross-lot consumption requires multiple events (one per lot) bundled in one envelope (or one SuperBatch).

### 11.5 Units (serial numbers)

Unit identity is the strictest pool granularity: one pool per (sku, location, unit_id). Each unit has a single cost layer (the cost at which the unit was received).

The dispatcher routes unit-tracked events to a unit-specific trait implementation (essentially specific-id costing). The pool's layer count is always 1 (or 0 if the unit has been consumed).

### 11.6 BOM orchestration

The caller is responsible for BOM expansion (§4.2). The committer doesn't do speculative expansion.

BOM expansion produces a list of component consumption events bundled into the WO completion envelope. The trait method for `wo_complete` processes them as a batch in `plan_apply`:

```rust
fn plan_apply_wo_complete(&self, batch: &PostingApplyRequest, snapshot: &Snapshot) -> ApplyResult {
    let wo = batch.work_order();
    let components = wo.bom_components();  // already expanded by caller, present in batch
    let outputs = wo.finished_outputs();

    // Consume components (per-component plan_apply)
    let mut total_input_cost = 0;
    for component in components {
        let consume_result = consume_from_pool(component.sku, component.qty, snapshot);
        total_input_cost += consume_result.applied_total_cost;
        // collect depletion rows etc.
    }

    // Produce outputs (layer inserts at calculated unit cost)
    let unit_cost = total_input_cost / outputs.total_qty();
    for output in outputs {
        emit_layer_insert(output.sku, output.qty, unit_cost);
    }

    // ...
}
```

The complexity here is in business logic, not in concurrency. Lex-locking has already ensured all the relevant pools are locked.

### 11.7 Cost books

Per design-v2's multi-cost-book design (acct mig 0030), a SKU can have multiple cost books simultaneously (e.g., a primary FIFO book and a secondary standard-cost book for management reporting). Each book is an additional pool dimension.

In v2.1, this means pool_keys carry cost_book_id, and pool_locks variants exist per book.

---

## 12. Webhook Delivery and Status Observation

### 12.1 At-least-once webhook delivery

The webhook delivery is a separate concern from envelope processing. The committer's Step 5 inserts `webhook_deliveries` rows; a dedicated webhook delivery worker picks them up and POSTs to the application tier's registered URLs.

**Network calls MUST NOT execute inside an open SPI transaction.** A synchronous `http_post` call to a misbehaving webhook target may hang for the full TCP timeout. If that call is inside a transaction holding `FOR UPDATE SKIP LOCKED` row locks, the transaction stays open for the duration, holding locks AND pinning PG's global `xmin` horizon. The result is severe MVCC bloat on every table touched by long-running transactions cluster-wide — not just on `webhook_deliveries`. The worker therefore uses a strict three-phase pattern with a fresh transaction per phase:

```rust
// Webhook delivery worker loop
loop {
    // ─── Phase 1: claim due rows. Tx opens, claims, commits. ───────────────
    StartTransactionCommand();
    let due = spi_execute(
        "WITH claimed AS (
             SELECT delivery_id, correlation_id, payload, target_url, attempt_count
               FROM webhook_deliveries
              WHERE state = 'pending'
                AND next_attempt_at <= now()
              ORDER BY next_attempt_at ASC
              LIMIT 100
              FOR UPDATE SKIP LOCKED
         )
         UPDATE webhook_deliveries SET
             state = 'in_flight',
             attempt_count = webhook_deliveries.attempt_count + 1,
             claimed_at = now()
         FROM claimed
         WHERE webhook_deliveries.delivery_id = claimed.delivery_id
         RETURNING webhook_deliveries.delivery_id, claimed.correlation_id,
                   claimed.payload, claimed.target_url,
                   webhook_deliveries.attempt_count"
    );
    CommitTransactionCommand();
    // Locks released here. xmin horizon free to advance.

    if due.is_empty() {
        sleep(webhook_poll_interval_ms);
        continue;
    }

    // ─── Phase 2: synchronous http_post per delivery. NO open tx. ──────────
    // Each http_post may take up to its TCP timeout. Holding NO database
    // resources during this phase is what makes the pattern safe.
    let outcomes: Vec<(DeliveryId, Outcome)> = due.iter().map(|d| {
        let outcome = match http_post(d.target_url, d.payload) {
            Ok(_) => Outcome::Delivered,
            Err(e) if d.attempt_count < webhook_max_attempts => {
                Outcome::Retry { error: e.to_string() }
            }
            Err(e) => Outcome::PermanentFailure { error: e.to_string() },
        };
        (d.delivery_id, outcome)
    }).collect();

    // ─── Phase 3: write terminal/backoff state. Fresh tx, commits, done. ───
    StartTransactionCommand();
    for (delivery_id, outcome) in outcomes {
        match outcome {
            Outcome::Delivered => {
                spi_execute(
                    "UPDATE webhook_deliveries SET state = 'delivered',
                                                  delivered_at = now()
                       WHERE delivery_id = $1",
                    delivery_id
                );
            }
            Outcome::Retry { error } => {
                // backoff_ms = webhook_backoff_base_ms × 2^(attempt_count - 1)
                spi_execute(
                    "UPDATE webhook_deliveries SET
                         state = 'pending',
                         next_attempt_at = now() + (webhook_backoff_base_ms *
                             POWER(2, attempt_count - 1) || ' ms')::interval,
                         last_error = $1
                       WHERE delivery_id = $2",
                    error, delivery_id
                );
            }
            Outcome::PermanentFailure { error } => {
                spi_execute(
                    "UPDATE webhook_deliveries SET state = 'permanent_failure',
                                                  last_error = $1,
                                                  failed_at = now()
                       WHERE delivery_id = $2",
                    error, delivery_id
                );
            }
        }
    }
    CommitTransactionCommand();
}
```

**Crash safety across phases.** A worker crash between Phase 1 and Phase 3 leaves rows stuck in `state = 'in_flight'` with stale `claimed_at`. A periodic sweep (every `webhook_orphan_sweep_interval_ms`, default 60s) reverts in_flight rows where `now() - claimed_at > webhook_in_flight_max_ms` (default 5 min — long enough to cover normal TCP timeouts plus margin) back to `state = 'pending'` with `next_attempt_at = now()`. The sweep is a single bulk UPDATE in its own short transaction; no network I/O.

**At-least-once semantics.** A worker crash after http_post returns success but before Phase 3 commits leaves the row at `in_flight` until the orphan sweep flips it back to `pending`; the webhook will be re-sent. The receiving application MUST treat webhook delivery as at-least-once and deduplicate by `correlation_id`. This is documented in the webhook contract (§12.3).

**Exponential backoff with cap.** Permanent failure after `webhook_max_attempts` (default 10).

### 12.2 The status observation API

For application tiers that can't or don't want to handle webhooks reliably, polling is the fallback:

```sql
SELECT * FROM ledger_submission_status WHERE correlation_id = $1;
```

Returns state, error_code, error_detail, and timing fields. The application can poll this for envelopes it cares about.

For high-volume status queries, the partial index on `state` keeps the hot path (queued + processing) fast.

### 12.3 Webhook URL registration

The extension exposes a configuration table:

```sql
CREATE TABLE webhook_subscriptions (
    subscription_id BIGSERIAL PRIMARY KEY,
    target_url      TEXT NOT NULL,
    event_filter    JSONB NOT NULL,   -- which event_types this URL receives
    active          BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

The committer's Step 5 looks up matching subscriptions when inserting webhook_deliveries rows; one delivery row per matching subscription per envelope.

**Flagged concern (§14 A16):** webhook URLs and HTTP semantics are heavy machinery to put in a DB extension. Alternative: don't deliver webhooks from the extension; expose only the `ledger_submission_status` table and rely on the application tier to poll. Lighter extension; application tier carries more responsibility. Test webhook delivery as the default; document the polling-only alternative.

---

## 13. GUCs, Monitoring, Operational Concerns

### 13.1 GUCs (already listed in §5.5; reproduced for reference)

| GUC | Default | Reload |
|-----|---------|--------|
| `ledger.staging_queue_size` | 65536 | Postmaster |
| `ledger.committer_queue_size` | 8192 | Postmaster |
| `ledger.spillover_arena_mb` | 256 | Postmaster |
| `ledger.queue_full_timeout_ms` | 5000 | Sighup |
| `ledger.committer_lease_ms` | 100 | Sighup |
| `ledger.committer_count` | 4 | Sighup |
| `ledger.router_window_size` | 1000 | Sighup |
| `ledger.batch_size_max` | 50 | Sighup |
| `ledger.batch_window_us` | 500 | Sighup |
| `ledger.user_tx_coupling` | strict | Sighup |
| `ledger.status_insert_mode` | caller_intx | Sighup |
| `ledger.max_eject_count` | 10000 | Sighup |
| `ledger.caller_tx_timeout_ms` | 30000 | Sighup |
| `ledger.snapshot_layer_limit_per_pool` | 1000 | Sighup |
| `ledger.webhook_max_attempts` | 10 | Sighup |
| `ledger.webhook_backoff_base_ms` | 100 | Sighup |
| `ledger.persistent_staging` | off | Postmaster |
| `ledger.persistent_staging_gc_retention_hours` | 24 | Sighup |
| `ledger.router_starvation_threshold_ticks` | 10 | Sighup |
| `ledger.close_hook_drain_poll_ms` | 100 | Sighup |
| `ledger.close_hook_drain_timeout_ms` | 1800000 | Sighup |

**Tuning guidance:**

- `committer_lease_ms` must be sized for the storage hardware: `committer_lease_ms = max(100, 10 × fsync_p99_ms)`. The default 100ms assumes NVMe-class storage with p99 fsync latency under 10ms. On slower storage, increase to avoid false orphan recovery during legitimately-slow commits. Measure fsync_p99 with `pg_test_fsync` before tuning.
- `committer_count` should match the I/O parallelism the workload needs. Start at 4 and scale by measurement. Each committer is a dedicated BGWorker; postmaster restart restarts them all.
- `status_insert_mode` defaults to `caller_intx` (cheapest, correct under restart). Set to `committer_lazy` only when paired with `persistent_staging=on` (for cheapest enqueue AND envelope durability under caller abort). Never set `committer_lazy` without `persistent_staging=on` — the extension fails to load with a hint pointing to this rule. The historical `caller_subtx` mode is no longer supported (see §4.4.1 "Why caller_subtx is not a supported mode").
- **Cold-start consideration**: on first contact with a new pool, the committer lazily INSERTs into `pool_locks` / `wac_pool_state` / etc. under ON CONFLICT DO NOTHING. Two committers racing to create the same row will serialize briefly. For workloads that touch many new pools per second sustained, consider pre-creating lock rows at SKU/WO setup time (eager creation, §14 A3). For batch-mode or steady-state workloads, lazy creation is fine.

### 13.2 Monitoring

Per-queue stats SQL functions:

```sql
SELECT * FROM ledger_staging_stats();
-- columns: state, count, oldest_age_seconds
SELECT * FROM ledger_committer_stats();
SELECT * FROM ledger_shmem_usage();
SELECT * FROM ledger_webhook_delivery_stats();
SELECT * FROM ledger_router_stats();
-- columns: ticks_per_sec, avg_pack_efficiency_pct, starvation_count
SELECT * FROM ledger_method_dispatch_stats();
-- per (method, event_type): plan_apply calls, errors, total time
```

These functions read shmem counters and `ledger_submission_status` to produce operational dashboards.

### 13.3 Bottleneck classification

Each operational period (e.g., 5min window), the system labels itself with the binding bottleneck for capacity planning:

- **B1 WAL**: WAL fsync rate is saturated. Mitigate via larger batch_size_max (more events per commit).
- **B2 SPI**: committer is SPI-bound. Mitigate via fewer SPI calls per batch (likely already optimal at 5-7 per SuperBatch).
- **B3 Lock contention**: cross-SuperBatch lex-lock waits dominate. Within-SuperBatch overlap is colocated by affinity grouping (§6.2) and absorbed by shared-snapshot plan_apply (no waits within one SuperBatch). Cross-SuperBatch contention only occurs when (a) a single connected component exceeds `batch_size_max` and is split into chunks, or (b) envelopes touching a shared pool arrive across router-tick boundaries. Mitigate via larger batch_size_max (more overlap colocated) or shorter batch_window_us (smaller temporal-boundary gaps).
- **B4 Router-bound**: router can't pack fast enough; staging queue fills. Mitigate via wider router_window_size.
- **B5 Webhook delivery**: webhook_deliveries backlog grows. Mitigate via more webhook workers, faster downstream consumers.
- **CPU-other**: residual; profile.

### 13.4 Operational runbooks

- **Stuck envelope**: query ledger_submission_status for envelopes in queued/processing for > X minutes. Diagnose via shmem stats and committer/router logs.
- **Webhook backlog**: query webhook_deliveries by state. Increase webhook_max_attempts or scale downstream.
- **Close hook stuck**: query in-flight envelopes with business_date in the closing period. Identify what's blocking drain.

---

## 14. Alternatives Flagged for In-Tandem Testing

Summary of the alternative-or-trade-off decisions flagged inline. Each represents a place where the spec makes a default choice but the alternative warrants validation under representative workload.

| ID | Decision | Default | Alternative | Test |
|----|----------|---------|-------------|------|
| A1 | depletion UNIQUE constraint | (issue_id, method_used, layer_id) | add (sku_id, location_id) to constraint; OR separate issue_id_registry | property test for caller-bug issue_id reuse |
| A2 | acct cutover model | atomic | tolerate concurrent legacy writers via stronger committer locks | bake-off both modes |
| A3 | lock row creation | lazy on first access | eager at SKU/WO creation | latency comparison on first-event path |
| A4 | R7 document unit_cost | webhook delivery | dedicated document_unit_cost_results table | durability vs simplicity trade |
| A5 | sole-writer enforcement | hard | advisory-lock soft / multi-writer with row locks | test in acct integration |
| A6 | BOM expansion location | caller-side | extension helper function `ledger_expand_pool_keys` | reduce duplication if painful |
| A7 | spillover allocator | freelist | slab | measure fragmentation under sustained workload |
| A8 | router fairness threshold | 10 ticks (defensive only; oldest-first group dispatch always claims head's group) | 5 / 20 / adaptive | resolved: affinity grouping with oldest-first dispatch eliminates the starvation case; threshold retained for future multi-router defensive use |
| A9 | router recovery sweep | superbatch_id pointer on staging entries (chosen; see §6.4) | full cross-queue walk fallback | resolved post-PoC: pointer-based sweep adopted |
| A10 | per-envelope failure isolation | yes (default) | atomic-group support | depends on application workflows |
| A11 | caller-tx-status wait bound | finite (drop after) | indefinite with capped poll | measure stuck-tx incidence |
| A12 | staging durability | per-envelope choice via `durable_queue` parameter | system-wide on/off via `persistent_staging` GUC (gates `durable_queue=true` availability) | bake-off measures overhead delta as a function of durable_queue request rate |
| A13 | close hook lock acquisition | per-pool sequential | all-upfront in lex order | concurrency impact during close |
| A15 | pool_locks variants | separate per identity dim | unified table with nullable columns | NULL semantics in keys |
| A16 | webhook delivery | extension-internal | extension exposes status table only; app polls | extension surface size vs reliability |
| A17 | SuperBatch composition | Affinity grouping (union-find on pool_key overlap), one component = one SuperBatch, split at batch_size_max as fallback (chosen) | router-enforced disjointness (fans overlap to separate SBs) OR FIFO packing (relies on consecutive arrival of overlapping envelopes) | resolved: disjointness was inefficient (inter-committer FOR UPDATE on every overlap); FIFO was incorrect under concurrent submission (overlapping envelopes interleave with non-overlap, split by arrival accident); affinity grouping actively routes by state-key to one committer |
| A18 | status_insert_mode `caller_subtx` | not supported | autonomous-tx via dblink/pg_background | resolved: PG sub-tx is a savepoint not autonomous; abort-survival was non-deliverable; `committer_lazy + persistent_staging` covers the use case |

Each requires either (a) implementing both and switching between via GUC, or (b) implementing the default and adding the alternative as a follow-up if data warrants. The follow-on PoC after the queue-primitives PoC is the right venue for these.

---

## 15. Open Issues

### 15.1 Q-A: Router-committer coordination — RESOLVED

The router's CAS-to-routed and the committer's CAS-to-in-flight are independently safe because they act on different shmem locations (router CAS on staging entry's `valid`, committer CAS on committer queue entry's claim slot, which stores (committer_slot, committer_token) plus `valid`). The data-before-flag ordering (Release store of `superbatch_id` before the routed CAS, Acquire load by the recovery sweep) handles the cross-shmem visibility concern. The PoC's `test_v21_router_release_acquire_ordering` test validates the pattern under simulated crash interleavings. No outstanding subtle ordering requirements identified.

### 15.2 Q-B: Cross-cost-book closing

Per design-v2's deferred multi-cost-book work (acct-zf80), the close hook may need to close multiple books in coordination (e.g., primary FIFO book and a secondary standard book that derives variances). v2.1's drain-to-zero needs to account for this: drain must include all books, not just the closing one.

### 15.3 Q-C: Webhook payload schema

The spec doesn't define the JSON schema for webhook payloads. The payload needs to include: correlation_id, terminal state, per-envelope error details (if failed), summary of cost rows committed (for the success case). Define alongside webhook URL registration design.

### 15.4 Q-D: Permissions and RLS

Who can call `ledger_enqueue`? Who can query `ledger_submission_status`? Who can register webhook subscriptions? The spec doesn't define a permission model. acct likely has an existing role hierarchy; the extension should integrate, e.g., GRANT EXECUTE ON ledger_enqueue TO acct_application_role.

### 15.5 Q-E: Cross-database / multi-tenant

Does this extension run in a single PG database, or does it support multi-tenant via schema-per-tenant? acct's tenant model determines the answer. If multi-tenant: pool_locks, ledger_submission_status, etc. need a tenant_id column; lex-locking sorts within a tenant; cross-tenant work is forbidden.

### 15.6 Q-F: Replication and HA

The current spec assumes a single primary. PG logical replication can stream the cost tables to a replica; shmem state cannot be replicated. Failover means losing all in-flight envelopes that haven't reached terminal state. The recovery sweep on the new primary's startup will mark them failed.

Tighter HA: persistent staging (§9.4 alternative A12) plus logical replication of the persistent staging table makes failover survive in-flight envelopes. Out of scope for the initial implementation but worth specifying for production deployments.

### 15.7 Q-G: Schema versioning of cost rows

If the extension's row format evolves (new column, changed semantics), how do existing cost rows interact with new code? Standard schema migration applies, but for tables this hot, online migrations need care. Beyond the initial implementation.

### 15.8 Q-H: GC of fully-reconciled state

Over time, `ledger_submission_status` and `webhook_deliveries` accumulate completed rows. A GC job (acct-9lx7-style) should archive old rows past some retention horizon. Define retention policy.

---

## Appendix: Comparison with design-v2

|  | design-v2 | v2.1 |
|---|---|---|
| Sharding boundary | per-pool (item, location) | per-document (envelope) |
| Atomicity unit | per-event | per-envelope |
| Lock acquisition | implicit (per-shard committer holds one pool at a time) | explicit (FOR UPDATE on all pools in batch) |
| Write pattern | per-event-group INSERT batches | one bulk UNNEST per SuperBatch |
| Caller coupling | independent (caller user-tx and committer user-tx are separate) | synchronous enqueue with user_tx_xid coupling |
| Failure recovery | per-shard orphan recovery with pg_xact | per-SuperBatch opportunistic with same mechanism |
| Idempotency | per-(issue_id, method) dedup at committer | same |
| Best fit | high-frequency low-document-overlap streaming events | document-level transactions with low cross-document SKU overlap |
| Bottleneck under contention | per-shard committer serialization | per-SuperBatch FOR UPDATE serialization |
| Concurrency-vs-atomicity trade | concurrency-first | atomicity-first |

Both are valid targets. The PoC validates the shared primitives (queue+committer mechanics, lease/orphan recovery, dedup). Choosing between v2 and v2.1 for a given acct workload is a data-driven decision based on workload characteristics (document overlap rate, transaction granularity, atomicity requirements).

---

## End of Specification

This document is the target architecture for v2.1. It is not yet implementation-validated; the follow-on PoC (separate from `/mnt/user-data/outputs/poc-validation-spec.md` which validates v2's queue primitives) should validate v2.1-specific items including bulk-UNNEST performance, router affinity-grouping correctness and overhead, lex-lock contention behavior, drain-to-zero close coordination, and the alternatives flagged in §14.
