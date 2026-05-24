# design-v3: PoC for cost-ledger architecture

## 1. Purpose

Establish the schema and two implementation paths for the cost-ledger, then measure both under varying workloads to determine where each path wins.

Path A is direct: caller's user-tx does the ledger work inline via a pgrx SPI function. One PG transaction per caller submission. Lock contention is caller-to-caller.

Path B is routed: caller stages a submission in shmem, returns immediately. Router (shmem) groups submissions by pool overlap. Committer pool (BGWorkers) processes commit_groups, each in one PG tx with batched writes. The trx row is created by the committer at successful processing time; existence of the trx row is the only durable signal that a submission was recorded.

Both paths share the same schema, the same Rust transformation core, and the same correctness guarantees.

## 2. Schema

Greenfield. No migration from existing acct schema.

### 2.1 Enums

```sql
CREATE TYPE pool_method AS ENUM ('fifo','lifo','wac','wac_periodic','std','specific');

CREATE TYPE trx_type AS ENUM (
    'po_receipt',
    'wo_completion',
    'inv_adjustment',
    'transfer_shipment',
    'transfer_receipt',
    'manual_adjustment',
    'revaluation_run'
);

CREATE TYPE line_type AS ENUM (
    'po_receipt_line',
    'wo_output',
    'wo_backflush',
    'wo_scrap',
    'inv_adjustment_line',
    'transfer_shipment_line',
    'transfer_receipt_line',
    'manual_adjustment_line',
    'revaluation_line'
);

CREATE TYPE posting_event_type AS ENUM (
    'inventory_receipt',
    'inventory_depletion',
    'wip_movement',
    'variance',
    'scrap',
    'adjustment',
    'revaluation'
);

CREATE TYPE account_type AS ENUM (
    'asset',
    'liability',
    'equity',
    'revenue',
    'expense'
);

CREATE TYPE dimension_type AS ENUM (
    'cost_center',
    'project',
    'department',
    'customer',
    'vendor'
);
```

### 2.2 Cost-ledger tables

```sql
CREATE TABLE pool (
    id            BIGINT PRIMARY KEY,
    sku_id        BIGINT NOT NULL,
    location_id   BIGINT NOT NULL,
    identity_key  BIGINT NOT NULL DEFAULT 0,
    method        pool_method NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (sku_id, location_id, identity_key)
);

CREATE TABLE pool_state (
    pool_id            BIGINT NOT NULL REFERENCES pool(id),
    layer_seq          BIGINT NOT NULL,
    qty                BIGINT NOT NULL,
    unit_cost          BIGINT NOT NULL,
    last_trx_line_id   BIGINT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pool_id, layer_seq)
);

CREATE TABLE pool_lock (
    pool_id     BIGINT PRIMARY KEY REFERENCES pool(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE trx (
    id           BIGINT PRIMARY KEY,
    trx_type     trx_type NOT NULL,
    source_id    BIGINT NOT NULL,
    posted_at    TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (trx_type, source_id)
);

CREATE TABLE trx_line (
    id                  BIGINT PRIMARY KEY,
    trx_id              BIGINT NOT NULL REFERENCES trx(id),
    pool_id             BIGINT NOT NULL REFERENCES pool(id),
    line_type           line_type NOT NULL,
    source_id           BIGINT,
    qty                 BIGINT NOT NULL,
    unit_cost           BIGINT NOT NULL,
    trx_seq             BIGINT NOT NULL,
    source_trx_line_id  BIGINT REFERENCES trx_line(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pool_id, trx_seq)
);

CREATE INDEX trx_line_trx ON trx_line (trx_id);
CREATE INDEX trx_line_pool ON trx_line (pool_id);
CREATE INDEX trx_line_source ON trx_line (source_trx_line_id) WHERE source_trx_line_id IS NOT NULL;
```

### 2.3 Journal-side tables

```sql
CREATE TABLE account (
    id          BIGINT PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    type        account_type NOT NULL,
    parent_id   BIGINT REFERENCES account(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE posting_line (
    id              BIGINT PRIMARY KEY,
    trx_line_id     BIGINT NOT NULL REFERENCES trx_line(id),
    event_type      posting_event_type NOT NULL,
    amount          BIGINT NOT NULL,
    debit_account   BIGINT NOT NULL REFERENCES account(id),
    credit_account  BIGINT NOT NULL REFERENCES account(id),
    posted_at       TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX posting_line_trx_line ON posting_line (trx_line_id);
CREATE INDEX posting_line_posted_at ON posting_line (posted_at);

CREATE TABLE posting_line_dimension (
    posting_line_id  BIGINT NOT NULL REFERENCES posting_line(id),
    dimension_type   dimension_type NOT NULL,
    dimension_id     BIGINT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (posting_line_id, dimension_type)
);

CREATE INDEX posting_line_dimension_lookup ON posting_line_dimension (dimension_type, dimension_id);
```

### 2.4 Period and reference tables

```sql
CREATE TABLE accounting_period (
    id          BIGINT PRIMARY KEY,
    start_date  DATE NOT NULL,
    end_date    DATE NOT NULL,
    state       TEXT NOT NULL CHECK (state IN ('open','closing','closed')),
    closed_at   TIMESTAMPTZ,
    closed_by   TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (start_date, end_date)
);

CREATE TABLE sku (
    id          BIGINT PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE location (
    id          BIGINT PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

For PoC scope, sku and location carry the minimum needed for the ledger to reference them. Production schemas would extend with all the domain attributes (units of measure, descriptions, tax categories, etc.).

### 2.5 Periodic-WAC accessory table

Used by `wac_periodic` pools (§3.6) and `ledger_close_period` (§4.5). One row per `wac_periodic` depletion captures the provisional amount posted at the running pool average at depletion time, so the close hook can recompute variance per row against the period's final_avg.

```sql
CREATE TABLE posting_lines_provisional (
    id                          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    posting_line_id             BIGINT NOT NULL REFERENCES posting_line(id),
    pool_id                     BIGINT NOT NULL REFERENCES pool(id),
    qty                         BIGINT NOT NULL,                    -- absolute depletion qty
    provisional_amount          BIGINT NOT NULL,                    -- amount posted at running pool avg at depletion time
    finalized_at                TIMESTAMPTZ,                        -- NULL until close hook finalizes
    variance_amount             BIGINT,                             -- (final_avg × qty) − provisional_amount; signed; may be 0
    variance_posting_line_id    BIGINT REFERENCES posting_line(id), -- NULL when variance == 0 or row unfinalized
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (posting_line_id),
    CHECK (
        (finalized_at IS NULL  AND variance_amount IS NULL  AND variance_posting_line_id IS NULL)
        OR (finalized_at IS NOT NULL AND variance_amount IS NOT NULL)
    )
);

CREATE INDEX posting_lines_provisional_pool_unfinalized
    ON posting_lines_provisional (pool_id)
    WHERE finalized_at IS NULL;
```

`period_id` is not stored: the close hook resolves period coverage via `posting_line.posted_at` against `accounting_period (start_date, end_date)` at close time. Trades a join for not having to plumb `period_id` through the SPI signature or denormalize it onto every depletion.

## 3. Method semantics

Each pool_method determines how trx_line rows interact with pool_state. The Rust core (ledger-core) implements one plan_apply per method.

### 3.1 WAC

pool_state has exactly one row per pool, at layer_seq = 0. The row carries the pool's total `qty` and a **cumulative `value_sum`** — stored in the column physically named `unit_cost` per the per-method storage contract (§3.7). The running per-unit cost is `value_sum / qty`, computed on demand; never stored.

**Receipt** of qty Q at unit_cost C:
- Allocate trx_seq from the pool's lock-held sequence.
- INSERT trx_line (qty=Q, unit_cost=C, trx_seq) — trx_line.unit_cost is the caller-supplied input.
- UPSERT pool_state at layer_seq=0:
  - If row doesn't exist: insert with `qty = Q`, `value_sum = Q × C`.
  - Otherwise: `qty += Q`, `value_sum += Q × C`. EXACT additive update — no rounding, no division.

The receipt step is commutative and associative across concurrent commit_groups: any order of `(qty, value_sum) += (Q_i, Q_i × C_i)` updates converges to the same `(Σ qty_i, Σ Q_i × C_i)` total. This is the load-bearing property that lets Path A and Path B produce byte-identical pool_state for receipt-only workloads even when Path B reorders commit_groups across committers (see §10.4 crossover characterization).

**Depletion** of qty Q (Q > 0):
- Read pool_state at layer_seq=0 under the pool's FOR UPDATE lock. If `qty < Q`, error InsufficientInventory.
- Compute `amount = (Q × value_sum) / qty` — SINGLE bounded round per depletion (integer division). This is the only rounding event in the WAC lifecycle.
- Allocate trx_seq.
- INSERT trx_line (qty=-Q, unit_cost = amount / Q, trx_seq) — trx_line.unit_cost is a display/audit field carrying the per-unit cost at depletion; the value-preserving column is posting_line.amount = the rounded amount.
- UPDATE pool_state at layer_seq=0: `qty -= Q`; `value_sum -= amount` — exact subtraction on whatever the amount rounded to.
- INSERT posting_line with the rounded amount.

Because the post-depletion `value_sum` is exact subtraction of the rounded amount, drift does not compound: subsequent depletions see a `value_sum` that already accounts for the prior per-depletion rounding. Cross-path drift in mixed receipt+depletion workloads is bounded per-depletion (|Δ| ≤ 4 units in the worst case observed under stress; see acct-h5gs + acct-mcey), not linear in receipt count.

**Why no divide-by-zero guard.** Under cumulative-sum the receipt step has no division. The depletion step's `(Q × value_sum) / qty` is gated by `qty >= Q > 0`, so `qty > 0` at the divide. Both directions are safe by construction — the prior running-average formulation's load-bearing `new_qty <= 0` guard is no longer needed.

### 3.2 FIFO

pool_state has one row per live receipt layer at layer_seq = trx_seq of the receipt. Rows DELETE when qty reaches zero.

**Receipt** of qty Q at unit_cost C:
- Allocate trx_seq.
- INSERT trx_line (qty=Q, unit_cost=C, trx_seq).
- INSERT pool_state (layer_seq=trx_seq, qty=Q, unit_cost=C, last_trx_line_id=trx_line.id).

**Depletion** of qty Q:
- Read pool_state ORDER BY layer_seq ASC. Take layers until accumulated qty >= Q.
- For each layer touched, allocate a trx_seq and INSERT trx_line:
  - qty = -consumed_from_this_layer
  - unit_cost = layer.unit_cost
  - source_trx_line_id = layer's receipt trx_line id (lookup via pool_state.last_trx_line_id or by joining)
- For each layer touched, UPDATE pool_state.qty -= consumed (or DELETE if hits zero).

### 3.3 LIFO

Same as FIFO but ORDER BY layer_seq DESC.

### 3.4 STD

No pool_state rows. The standard cost lives in a separate `standard_costs` table (out of v3 scope; assumed to exist as part of acct's domain).

**Receipt/Depletion**: INSERT trx_line with unit_cost = standard_cost. Variance posting_lines capture the difference between standard and actual on receipts (deferred to method semantics in implementation).

### 3.5 Specific-id

Each unit is its own pool (pool.identity_key = unit_id, pool.method = 'specific'). The pool has one layer with qty=1 from its receipt; depletion of that unit consumes the entire layer (qty becomes 0, row DELETEd). Same shape as FIFO with K=1.

### 3.6 WAC-periodic (Oracle PAC convention)

Same storage layout and depletion math as WAC (§3.1): `pool_state` has exactly one row per pool at `layer_seq = 0`, carrying `qty` and the cumulative `unit_cost` (= value_sum). The only behavioral difference from `wac` is that depletions are flagged as **provisional** for later restatement at period close.

**Receipt** of qty Q at unit_cost C:
- Identical to `wac` (§3.1). `qty += Q`, `unit_cost += Q × C` (exact additive). Receipts in periodic-WAC are not provisional — their amount IS `Q × C` directly, and the close hook consumes the in-period receipt rows from `posting_line` (event_type = 'inventory_receipt') joined to `trx_line` for qty, summing per pool to derive final_avg.

**Depletion** of qty Q (mid-period):
- Same math as `wac` depletion: `amount = (Q × unit_cost) / qty` (single bounded round); `qty -= Q`; `unit_cost -= amount`.
- ALSO: INSERT a `posting_lines_provisional` row (per §2.5) capturing `(posting_line_id, pool_id, qty=Q, provisional_amount=amount)` with `finalized_at = NULL`.
- At period close, `ledger_close_period` (§4.5) walks unfinalized rows, recomputes `final_avg = Σ(in-period receipt value) / Σ(in-period qty)` per pool, and posts variance per provisional row.

A pool with provisional depletions in the period but no in-period receipts cannot have a final_avg computed; the close hook raises an error (main acct's analog is P0020 `wac_periodic_close_no_receipts`).

### 3.7 Per-method storage contract for `pool_state.unit_cost`

The `pool_state.unit_cost` column physically holds different per-method semantics. Readers (including ad-hoc SQL) MUST dispatch on `pool.method` before interpreting the value.

| Method        | `qty` semantic         | `unit_cost` semantic                                                            |
|---------------|------------------------|---------------------------------------------------------------------------------|
| `fifo`        | per-layer qty          | per-layer unit_cost (caller-supplied at receipt; immutable across the layer)    |
| `lifo`        | per-layer qty          | same as fifo                                                                    |
| `specific`    | per-layer qty (always 1) | per-layer unit_cost (caller-supplied at receipt)                              |
| `wac`         | pool total qty (qty_sum) | cumulative `value_sum` = Σ(receipt_qty × receipt_unit_cost) − Σ(rounded depletion amount). Running per-unit cost = unit_cost / qty (computed on demand; never stored). |
| `wac_periodic`| same as `wac`          | same as `wac`                                                                   |
| `std`         | (no `pool_state` rows; standard cost lives in `standard_costs`)                                        |

See §13.8 for the operational footgun this creates for ad-hoc SQL.

## 4. Path A: direct write

### 4.1 SPI surface

One function, callable inside the caller's user-tx:

```rust
ledger_submit_trx(
    trx_type: trx_type,
    source_id: BIGINT,
    posted_at: TIMESTAMPTZ,
    lines: ARRAY of (line_type, source_id, pool_id, qty, unit_cost, debit_account, credit_account)
) RETURNS BIGINT  -- trx.id
```

Caller invokes inside their own user-tx, alongside whatever other work they're doing (e.g., updating po_receipt, wo_completion in their own domain schema). The function does the full ledger work synchronously.

### 4.2 Function logic

1. Allocate trx.id (BIGSERIAL or sequence).
2. Compute the set of pool_ids touched by `lines`. Sort ascending, dedup.
3. Acquire locks in singleton-loop sorted order: `SELECT 1 FROM pool_lock WHERE pool_id=$1 FOR UPDATE` for each. Lazy-create the lock row if absent via INSERT ON CONFLICT.
4. Bulk-read pool_state for all touched pools:
   ```sql
   SELECT pool_id, layer_seq, qty, unit_cost, last_trx_line_id
     FROM pool_state
    WHERE pool_id = ANY($1::bigint[])
    ORDER BY pool_id, layer_seq
   ```
   ORDER BY is explicit so layer-ordered methods (FIFO ASC, LIFO DESC) can rely on the layer ordering when ledger-core demultiplexes rows per pool. The PRIMARY KEY (pool_id, layer_seq) on pool_state makes the sorted scan free — it's an index-ordered traversal. ledger-core additionally re-sorts per-pool defensively before passing the snapshot to plan_apply; the database-side ORDER BY is the contract, the client-side sort is the enforcement.
5. Call ledger-core::plan_apply with the snapshot. Returns PlanResult.
6. If plan_apply errors (InsufficientInventory, etc.): RAISE EXCEPTION. The caller's user-tx aborts. No rows are written. The caller sees the SQL exception directly.
7. Otherwise apply PlanResult and write all rows:
   - INSERT trx (UNIQUE constraint on (trx_type, source_id) fires here if the receipt was already recorded by a prior caller; raises ERRCODE_UNIQUE_VIOLATION; caller's tx aborts).
   - Bulk INSERT trx_line rows.
   - Bulk INSERT pool_state rows (for layer-tracked receipts).
   - Bulk UPSERT pool_state rows (for WAC).
   - Bulk UPDATE pool_state rows (for partial depletions).
   - Bulk DELETE pool_state rows (for fully-consumed layers).
   - Bulk INSERT posting_line rows.
8. Return trx.id.

Caller's user-tx commits. One fsync. The trx row exists iff everything succeeded; if any step failed, the caller's tx aborted and nothing landed.

### 4.3 Concurrency

Multiple callers submitting concurrently to overlapping pools serialize at the FOR UPDATE on pool_lock. The first caller holds the lock for the duration of their plan_apply + writes; the second caller waits. Throughput is bounded by `1 / (lock_hold_time × pool_overlap_probability)` per pool.

No queue, no router, no committer pool, no shmem. Failure modes are caller-visible: the user-tx aborts on any failure, the caller sees the exception.

### 4.4 Per-trx SPI count

- 1 INSERT trx.
- P singleton FOR UPDATE on pool_lock (P = deduped pool count). Plus P ON CONFLICT INSERTs if any are new.
- 1 SELECT pool_state.
- 0-N writes per pool depending on what happened: bulk INSERT trx_line (1), bulk INSERT/UPSERT/UPDATE/DELETE pool_state (1-4 depending on operations), bulk INSERT posting_line (1).

Total: ~5-9 SPI calls + 2P singleton calls. For a single-line PO receipt with P=1: ~7 SPI calls. For a 5-line WO completion with P=6 (output + 5 components): ~17 SPI calls.

### 4.5 Period close (`ledger_close_period`)

Synchronous SPI in the caller's user-tx, mirroring `ledger_submit_trx`'s posture. Period close is a rare heavy operation; sync execution avoids a routed/async control plane for the PoC.

```rust
ledger_close_period(period_id: BIGINT) RETURNS JSONB
```

Returns a JSONB summary `{ period_id, provisional_finalized, variance_posted, zero_variance, pools_affected }`. Raises `ereport!(ERROR, ...)` and aborts the caller's tx on any failure.

**Pipeline:**

1. `SELECT period FOR UPDATE` — raise if `state != 'open'`.
2. `UPDATE state = 'closing'`.
3. `SELECT` unfinalized `posting_lines_provisional` joined to `posting_line`, filtered by `posted_at` within `[start_date, end_date + 1 day)`. If empty: trivial close (skip to step 11).
4. For each affected pool, acquire `pool_lock FOR UPDATE` in ascending pool_id order (same singleton-loop discipline as §4.2 step 3 / §5.4 step 6).
5. Compute per-pool `final_avg = Σ(in-period receipt value) / Σ(in-period qty)` from `posting_line` (event_type = 'inventory_receipt') joined to `trx_line` for qty. Pools with provisional depletions but zero in-period receipts → raise (cannot compute final_avg; main acct analog: P0020).
6. Per provisional row: `variance = final_avg × qty − provisional_amount` (signed).
7. INSERT one `revaluation_run` trx for the close + one variance trx_line per non-zero-variance provisional row (qty=0; unit_cost=variance/qty for audit). Allocate `trx_seq` per pool by reading `MAX(trx_seq)` once per pool under lock and incrementing.
8. INSERT variance `posting_line` rows:
   - `variance > 0`: same `(debit_account, credit_account)` as the original depletion; `amount = variance`.
   - `variance < 0`: SWAPPED `(debit_account, credit_account)`; `amount = |variance|`.
   - `variance = 0`: no posting (variance_posting_line_id stays NULL on the finalized row).
9. Adjust `pool_state.unit_cost` (= value_sum, per the per-method storage contract) by `−Σ variance` per pool — the cumulative-sum bookkeeping correction for re-stating depletions at final_avg instead of provisional_amount.
10. UPDATE each provisional row: `finalized_at = now()`, `variance_amount`, `variance_posting_line_id` (NULL when variance == 0).
11. UPDATE period: `state='closed'`, `closed_at=now()`.
12. Return JSONB summary.

**Variance routing in the PoC.** Variance flows through the SAME `(debit, credit)` accounts as the original depletion (with direction swap on negative variance). Main acct routes variance through a dedicated `variance_wac_periodic` account_kind. The PoC simplification is acceptable because the equivalence check operates on per-pool value totals — direct routing yields identical ledger end-state.

**Scope.** `ledger_close_period` implements close mechanics ONLY for `wac_periodic` pools (it walks `posting_lines_provisional`). Methods that drain to zero at close (FIFO/LIFO/specific layer cleanup, period revaluation runs for `std`, balance-sheet rollups) are deferred — see §12.

## 5. Path B: routed

### 5.1 SPI surface

```rust
ledger_enqueue_trx(
    trx_type: trx_type,
    source_id: BIGINT,
    posted_at: TIMESTAMPTZ,
    lines: ARRAY of (line_type, source_id, pool_id, qty, unit_cost, debit_account, credit_account)
) RETURNS BIGINT  -- submission_id (shmem-local; not trx.id)
```

Caller invokes inside their own user-tx (or as a standalone call). The function:

1. Pushes a descriptor (trx_type, source_id, posted_at, pool_ids touched, line data) to the shmem staging queue. Receives a shmem-local submission_id.
2. Returns submission_id.

No DB write at submission. The trx row is created only when the committer successfully processes the submission. If the committer fails or the postmaster crashes before processing, no trx row exists and the submission is lost — caller can resubmit. The submission_id is only meaningful for the lifetime of the shmem queue; once the committer creates the trx row (or fails), the submission_id is no longer valid.

Caller observability (polling for completion, knowing trx.id, error reporting) is out of scope for this PoC. The PoC measures committer throughput, not caller-side API design.

### 5.2 Shmem layout

The routed path needs shmem for the router and committer coordination. Carried forward from v2.1's design with v3 schema substituted.

- `staging_queue`: ring buffer of StagingEntry structs. Each entry holds trx.id, pool_id list, line payload (in arena).
- `committer_queue`: ring buffer of CommitterQueueEntry structs. Each entry represents a commit_group: a list of staging entries grouped by pool overlap.
- `spillover_arena`: variable-length payload storage (line arrays, pool_id arrays).
- `committer_identity_registry`: extension-owned shmem array tracking active committer BGWorkers.

State machines, recovery sweeps, eject mechanism, and CAS ordering carry over from v2.1 §5, §6, §9, §14 with the schema-side details updated.

### 5.3 Router

Background worker that scans the staging window every `batch_window_us` (default 500μs):

1. Read up to `router_window_size` (default 1000) pending entries from staging queue head.
2. Skip entries whose `eject_count > 0 AND now - last_eject_at_ns < eject_cooldown_ms` (default 10ms cooldown).
3. Build union-find by pool_id overlap. Each connected component becomes a candidate commit_group.
4. For components exceeding `batch_size_max` (default 50 trxs): split into chunks.
5. For each commit_group: CAS staging entries pending → processing → routed; push commit_group to committer queue.

Affinity grouping ensures overlapping trxs go to the same committer (avoids committer-vs-committer lock contention on hot pools).

### 5.4 Committer

Pool of BGWorkers (default 4). Each:

1. Claim a commit_group via CAS on committer queue entry (ready → in_flight). The shmem entry records the claiming committer's identity (slot + token from CommitterIdentityRegistry, see §5.5).
2. Open a PG tx (top-level, READ COMMITTED).
3. Read the commit_group's trxs and lines from shmem.
4. For Path B variants that carry caller's user_tx_xid in the shmem entry: check pg_xact_status:
   - 'committed': keep.
   - 'aborted': drop the submission from the batch (no trx will be created for it).
   - 'in_progress': eject (CAS staging entry back to pending, increment eject_count, store last_eject_at_ns, exclude). The cooldown prevents tight cycling.
5. Compute the union of pool_ids across all included submissions. Sort ascending, dedup.
6. Acquire pool_lock FOR UPDATE in singleton-loop order. Lazy-create with INSERT ON CONFLICT as needed.
7. Bulk-read pool_state for all touched pools:
   ```sql
   SELECT pool_id, layer_seq, qty, unit_cost, last_trx_line_id
     FROM pool_state
    WHERE pool_id = ANY($1::bigint[])
    ORDER BY pool_id, layer_seq
   ```
   Same query and ordering contract as Path A §4.2 step 4. The PRIMARY KEY (pool_id, layer_seq) makes the sort an index-ordered traversal. ledger-core demultiplexes per-pool and re-sorts defensively before plan_apply.
8. For each submission, dispatch to ledger-core::plan_apply with the snapshot. Apply results to a working snapshot. On plan_apply failure for a submission: exclude that submission (pristine-snapshot replay semantics), restart from pristine snapshot with the excluded set. Excluded submissions produce no trx row.
9. After clean pass: bulk INSERT trx (one row per included submission; UNIQUE constraint on (trx_type, source_id) fires if any submission is a duplicate of an already-recorded receipt — duplicate submissions abort the COMMIT, the committer logs the conflict, and on retry the conflicting submission is excluded), bulk INSERT trx_line, bulk INSERT/UPSERT/UPDATE/DELETE pool_state, bulk INSERT posting_line.
10. COMMIT. One fsync.
11. Step 11 cleanup: CAS staging entries routed → empty, free arena blocks. Three-case CAS handling per v2.1 §7.8 (success, ejected entries left at pending, router-died-mid-stamp entries cleaned via CAS 2→0).

One fsync per commit_group. With N submissions per commit_group, fsync cost amortizes by N. Each committed submission produces exactly one trx row; failed submissions produce nothing.

### 5.5 Recovery

- **Router death**: shmem boot sweep on router restart. Staging entries at processing (valid=2) get inspected: if their CommitterQueueEntry doesn't exist or is at empty, revert to pending; clear superbatch_id before CAS to avoid stale-value resurrection.
- **Committer death**: CommitterIdentityRegistry (extension-owned shmem array) tracks active committers via (slot, token). Liveness check is registry lookup. Dead committer's in-flight commit_group is reclaimed by next active committer via CAS on the queue entry. Whether the dead committer's PG tx committed is checked by looking up its xid in pg_xact:
  - Committed: the trx rows exist; the recovery committer doesn't reprocess; releases the shmem entries.
  - Aborted (or never started): the trx rows don't exist; the recovery committer reprocesses the commit_group normally.
- **Postmaster crash**: shmem is gone. All in-flight submissions are lost. No trx rows exist for them (they were never written). No DB cleanup is needed — the only thing in the DB are trx rows for previously-committed submissions, and those are durable and correct. Callers whose submissions were in flight at crash time observe the loss (their polling never sees a trx for that source) and resubmit.

### 5.6 Per-commit_group SPI count

For a commit_group with N submissions touching P deduped pools:

- P singleton FOR UPDATE on pool_lock + P lazy INSERT ON CONFLICT.
- 1 bulk pool_state read.
- ~7 bulk writes: trx INSERT, trx_line INSERT, pool_state INSERT (receipts), pool_state UPSERT (WAC), pool_state UPDATE (partial depletions), pool_state DELETE (consumed layers), posting_line INSERT.

Total: ~8 bulk + 2P singleton. For N=50 submissions at P=50 deduped pools: ~108 SPI calls per commit_group, amortizing to ~2.16 SPI/submission. For N=10 at P=10: ~28 SPI calls per commit_group, ~2.8 SPI/submission.

Compare Path A at ~7-17 SPI per trx. Path B amortizes by batching.

## 6. ledger-core (shared Rust crate)

```
ledger-core/
  src/
    method.rs        - PoolMethod enum, plan_apply dispatcher
    fifo.rs          - FIFO plan_apply
    lifo.rs          - LIFO plan_apply
    wac.rs           - WAC plan_apply (cumulative-sum storage; §3.1)
    wac_periodic.rs  - WAC-periodic plan_apply (provisional flagging; §3.6)
    standard.rs      - STD plan_apply (with variance) — file named `standard` to avoid shadowing ::std
    specific.rs      - Specific-id plan_apply
    layered.rs       - Shared layer-walk logic for FIFO/LIFO/specific
    seq.rs           - Per-pool monotonic trx_seq counter helper
    snapshot.rs      - Snapshot type (HashMap<pool_id, PoolStateRows>)
    plan.rs          - PlanResult (trx_line, pool_state mutations, posting_line, provisional_postings)
    error.rs         - LedgerError enum (InsufficientInventory, MethodMismatch, etc.)
```

Pure Rust. No pgrx dependency. Unit-testable in isolation.

Both ledger-direct (Path A) and ledger-routed (Path B) depend on ledger-core. They invoke the same plan_apply functions with the same Snapshot types and consume the same PlanResult outputs. Differences are entirely in how they acquire snapshots, write results, and manage transactions.

## 7. Pristine-snapshot replay

Carried forward from v2.1 §7.6. Applies to both paths.

When a trx's plan_apply errors during the working snapshot's evolution, the committer (or direct-path SPI function) does NOT delta-rollback. It:

1. Adds the failing trx to the excluded set.
2. Discards the working snapshot.
3. Clones the pristine (post-hydration, pre-plan_apply) snapshot.
4. Re-runs plan_apply for all non-excluded trxs in chronological order.
5. Repeats until a clean pass or all trxs excluded.

Naive delta-rollback corrupts AVG/FIFO state when subsequent trxs have already booked against the now-reverted intermediate state. Pristine-replay handles this correctly at the cost of O(replay_passes × events) work in the worst case. In practice 0 or 1 replay passes per commit_group.

For Path A with N=1 trx per call, this collapses: a failing trx just aborts the caller's user-tx. No replay needed. Pristine-replay only matters in Path B where commit_groups have N > 1.

## 8. Testing strategy

### 8.1 ledger-core unit tests

For each method (FIFO, LIFO, WAC, STD, specific), drive synthetic snapshots through plan_apply and assert PlanResult correctness:

- Single receipt into empty pool.
- Single depletion of available pool.
- Depletion exceeding available qty (InsufficientInventory).
- FIFO: depletion spanning multiple layers.
- WAC: divide-by-zero guard when depleting to qty=0 then receiving.
- WAC: avg calculation across mixed-cost receipts.
- Idempotent replay (same input → same output).

### 8.2 ledger-direct integration tests

Run against a real PostgreSQL with the schema installed. Test cases:

- Submit one trx, verify trx + trx_line + pool_state + posting_line all written and consistent.
- Submit FIFO receipt + depletion, verify layer math.
- Submit WAC receipts + depletions, verify running average.
- Insufficient inventory: verify error propagates as a SQL exception, caller's user-tx aborts, no trx or trx_line or pool_state rows are written.
- Duplicate submission: same (trx_type, source_id) submitted twice; second submission fails on UNIQUE constraint; caller's second tx aborts; first trx is intact.
- Concurrent callers (separate psql sessions) submitting to overlapping pools: verify serialization, no deadlocks, consistent final state.
- Concurrent callers to disjoint pools: verify true parallelism (no false serialization).

### 8.3 ledger-routed integration tests

Same as 8.2 plus:

- Submit, verify submission accepted; poll for trx existence by (trx_type, source_id); confirm trx appears after processing.
- Concurrent submissions to overlapping pools: verify affinity grouping (one commit_group handles all overlap, single committer per group).
- Committer death mid-processing: verify orphan recovery picks up the commit_group; trx rows for submissions in the group either all exist (recovery committer's tx committed) or all don't (recovery committer reprocesses fresh).
- Postmaster restart with in-flight submissions: verify trx rows from prior commits are intact; in-flight submissions are lost (no DB orphans); the system accepts new submissions and processes normally.
- Eject cycling: a caller's user-tx held open for several seconds; verify committer doesn't stall, eject_count incremented, eventually caller commits or wall-clock fires.
- Duplicate submission: same (trx_type, source_id) submitted twice; the second submission to reach Step 9 of §5.4 fails on UNIQUE constraint at INSERT trx; the committer logs the conflict; affected commit_group's tx rolls back; pristine-replay excludes the duplicate and re-runs.

### 8.4 Cross-path equivalence

Submit the same workload through both paths, verify final pool_state and trx_line are byte-identical (modulo timestamps and ids). This validates that both paths produce the same business result.

### 8.5 Harness-driven measurement

The workload generator (`ledger-harness`) drives both paths through identical workloads and records:

- Throughput: committed trxs/second.
- Caller latency: submit-to-acknowledge (Path A: caller's user-tx commit returns; Path B: enqueue function returns).
- Effective latency: submit-to-recorded (Path A: same as acknowledge; Path B: polling detects trx existence by (trx_type, source_id)).
- Lock wait time per caller (Path A).
- Eject count per submission (Path B).
- Submissions per commit_group (Path B).
- CPU burn per eject cycle (Path B): aggregate CPU time spent in router scan + committer pg_xact_status check per eject event. Detects when eject_cooldown is too short or in_progress callers are dominating worker time.
- fsync count per second (system-wide).
- WAL volume per trx.

## 9. Workload configurations

The harness simulates production conditions with varying parameters.

### 9.1 Caller concurrency

- 1 caller (baseline).
- 10 callers (light concurrent load).
- 50 callers (moderate).
- 200 callers (heavy).
- 1000 callers (stress).

Each caller is a separate PG session running a loop: submit trx, optionally poll until committed, repeat.

### 9.2 Pool universe and overlap

- Pool universe: 10,000 pools (e.g., 1000 SKUs × 10 locations).
- Overlap distribution:
  - Uniform: each caller picks a pool uniformly at random.
  - Zipfian (skewed): hot pools dominate; simulates real retail/manufacturing where a few SKUs are popular.
  - Disjoint: each caller has a private pool range (no overlap).

### 9.3 Trx complexity

- Simple: 1 line per trx (PO receipt of one item).
- Medium: 3-5 lines per trx (typical inv adjustment, partial WO).
- Complex: 10+ lines per trx (full WO completion with backflushes, transfers).

### 9.4 Method mix

- All WAC.
- All FIFO.
- Mixed: 60% WAC, 30% FIFO, 10% STD (representative of real deployments).

### 9.5 Workload matrix

For the bake-off: cross-product of caller concurrency × overlap × complexity × method mix. Not every combination needs full runs; pick representative scenarios:

- S1: Light concurrency, low overlap, simple trxs, all WAC. Baseline.
- S2: Heavy concurrency, high overlap, simple trxs, all WAC. Tests routing's lock-contention amortization.
- S3: Light concurrency, low overlap, complex trxs, mixed methods. Tests per-trx work intensity.
- S4: Heavy concurrency, Zipfian overlap, complex trxs, mixed methods. Stress-test, production-like.
- S5: Pathological — 1000 callers, all hitting one hot pool, simple trxs. Routing should win dramatically.
- S6: Pathological — 1000 callers, fully disjoint pools, simple trxs. Direct should win (no contention, lower overhead).

## 10. Success criteria

### 10.1 Correctness

Both paths produce identical pool_state and trx_line content for identical workloads. Verified by cross-path equivalence tests (§8.4). Any divergence is a critical bug.

### 10.2 Direct path baseline

Path A under S1 (single caller, simple trxs) establishes the single-threaded baseline throughput against which other scenarios are compared. There is no a priori target; the measurement IS the baseline.

### 10.3 Routed path scaling

Path B under S2 (heavy concurrency, high overlap) should demonstrate effective batching — commit_groups averaging multiple trxs per group rather than commit_groups of size 1. If commit_group sizes stay at 1, the router isn't earning its complexity and Path B reduces to Path A with extra overhead.

### 10.4 Crossover identification

Identify the concurrency × overlap region where Path B beats Path A. Above the crossover: routed wins. Below: direct wins.

The crossover location is what the PoC characterizes empirically. Hypothesis going in: there exists some concurrency × overlap regime where direct's single-fsync simplicity outweighs routed's batching overhead, and another regime where routed's amortized fsync and reduced lock contention dominate. The PoC's job is to find both regimes and the boundary between them.

### 10.5 Failure mode coverage

Path B handles these failure scenarios with the invariant that **trx exists iff successfully recorded**:

- **Caller user-tx aborts before committer processes** (Path B variant that carries caller's user_tx_xid in shmem): committer detects the abort, drops the submission from the batch. No trx row is created. Caller knows their tx didn't commit; nothing to look up.
- **Caller user-tx held open past caller_tx_timeout**: committer ejects with cooldown. After timeout, the submission is dropped. No trx row is created.
- **Committer dies mid-processing**: orphan recovery picks up the commit_group. If the dead committer's PG tx committed (some trx rows already exist), the recovery committer sees them via UNIQUE constraint conflicts and excludes those submissions from its replay. If the dead committer's PG tx aborted, no trx rows exist and the replay creates them fresh. Either way: each submission ends with exactly zero or one trx row.
- **Router dies mid-routing**: shmem entries get reverted to pending by the boot sweep. Submissions are re-routed. No DB effect.
- **Postmaster crash**: shmem lost. In-flight submissions lost. No DB orphans (no trx rows were written for in-flight work). Previously-committed trx rows are durable and intact.

In all cases: a trx row in the database represents a successfully recorded business event. Its absence represents either "never submitted" or "submitted but didn't complete." The schema doesn't distinguish those two cases — that's a queue-layer concern.

## 11. PoC implementation plan

Phases run in dependency order. Each phase delivers something testable before the next begins.

### Phase 1: ledger-core + schema

- DDL for all tables and enums.
- ledger-core Rust crate.
- Unit tests for each plan_apply method.
- Documentation of method semantics.

Deliverable: a Rust crate that can be exercised in isolation, producing correct PlanResults from synthetic snapshots.

### Phase 2: ledger-direct

- pgrx extension exposing ledger_submit_trx.
- Integrates ledger-core.
- Bulk write logic (UNNEST inserts, etc.).
- Pool_lock acquisition and lazy creation.
- Error handling: caller-visible failure mode.

Deliverable: a Postgres extension. Submit a trx via SPI, see the ledger update. Tests pass.

### Phase 3: ledger-harness, direct measurements

- Workload generator (separate binary, multi-session PG client).
- Configurable concurrency, overlap distribution, complexity.
- Drives Path A through scenarios S1-S6.
- Records measurements.

Deliverable: baseline throughput numbers for Path A across the workload matrix.

### Phase 4: ledger-routed

- Shmem layout: staging queue, committer queue, spillover arena, identity registry.
- Router BGWorker: window scanning, union-find, affinity grouping, commit_group assembly.
- Committer BGWorker pool: claim, dedup, hydrate, plan_apply, bulk write, commit.
- Pristine-snapshot replay logic.
- Recovery: router boot sweep, committer death handling, postmaster restart.
- Eject mechanism with cooldown.

Deliverable: full routed path operational. Submissions in shmem queue; postmaster crash loses in-flight submissions; no DB orphans.

### Phase 5: ledger-harness, routed measurements

- Add routed-path driver (submits via enqueue, polls for trx existence by (trx_type, source_id) to determine completion).
- Drives Path B through S1-S6.
- Records measurements.

Deliverable: routed-path throughput numbers across the workload matrix.

### Phase 6: comparison and characterization

- Run both paths against identical workloads.
- Build crossover map: concurrency × overlap → which path wins.
- Document findings.
- Identify regimes where neither path is ideal and surface as future work.

Deliverable: PoC report with empirical data on the direct-vs-routed tradeoff.

## 12. What's out of scope for this PoC

- Multi-currency. Single currency assumed. amount in posting_line is in one implied currency.
- account_balance denormalization. Balances derived via SUM(amount) at query time. Production deployments would maintain a denormalized table.
- Hot-path FIFO audit queries (full layer-history reconstruction). Direct query of trx + pool_state suffices; materialized views can be added later.
- Period close mechanics for non-wac_periodic methods. `ledger_close_period` (§4.5) implements close for `wac_periodic` pools (walks `posting_lines_provisional`, recomputes final_avg, posts variance). FIFO/LIFO/specific layer cleanup, `std` revaluation runs at close, and balance-sheet rollups are deferred — v2.1's §10 mechanics carry over when needed.
- Webhook delivery. v2.1's webhook_deliveries machinery is omitted; the PoC measures throughput, not external notification.
- Multi-tenant isolation. Single-tenant.
- Identity-key extended dimensions beyond what pool.identity_key carries. Lot-tracking and unit-tracking work; arbitrary user-defined dimensions on inventory (project, customer, etc.) are deferred — they could be added as additional columns on pool or via a separate identity-attribute table.

## 13. Concerns and open questions

### 13.1 trx_seq allocation strategy

Per-pool monotonic. Two options:

(a) MAX scan: `SELECT COALESCE(MAX(trx_seq), 0) FROM trx_line WHERE pool_id = $1 FOR SHARE` at hydration. Under the pool's FOR UPDATE lock, this is single-threaded. The (pool_id, trx_seq) UNIQUE index makes it a single index-leaf lookup. Cost: one read per pool at hydration.

(b) Stored next_trx_seq on pool: `UPDATE pool SET next_trx_seq = next_trx_seq + N WHERE id = $1 RETURNING next_trx_seq`. Avoids the MAX scan but adds a write to pool on every commit_group. Creates a write hot-spot on pool.

Recommend (a) for PoC. The UNIQUE (pool_id, trx_seq) index on trx_line supports index-backwards-scan for MAX queries, which PG's planner picks naturally. For typical pools (warm in shared_buffers), the scan is a single page seek.

If measurement shows MAX scans dominate Path A latency on deep historical data (pages cold, physical I/O per query), the first mitigation is adding an explicit DESC index:

```sql
CREATE UNIQUE INDEX trx_line_pool_seq_desc ON trx_line (pool_id, trx_seq DESC);
```

This forces the planner to read the leftmost leaf entry for a pool_id — single page seek regardless of historical depth. The cost is one additional index on a high-write table; ~10-15% additional WAL volume on trx_line inserts. Worth measuring before adding by default.

If even the DESC index isn't enough (extremely deep history, cold workload), switch to option (b) — stored next_trx_seq on pool. That trades the index lookup cost for a write hot-spot on pool. PoC measurement decides.

### 13.2 Pool registration

Two options:

(a) Explicit: an operator (or seed function) INSERTs into pool before any trx references it. trx submission errors if the pool doesn't exist.

(b) Lazy: ledger_submit_trx / ledger_enqueue_trx auto-creates pool on first reference using (sku_id, location_id, identity_key, method) provided by the caller.

Lazy is easier for development; explicit is safer for production (forces deliberate configuration). Recommend explicit for the PoC: it surfaces missing-pool errors as deliberate failures rather than silent auto-creation.

### 13.3 Stale pool_state rows after total depletion

Under WAC (and wac_periodic), when a pool's qty drops to zero, the pool_state row stays at `(qty=0, value_sum=0)` — the cumulative-sum form depletes both fields to zero exactly (modulo the per-depletion rounding residue, which can leave `value_sum` slightly non-zero but bounded; see §3.1). The next receipt establishes a fresh basis: `(qty=Q, value_sum=Q × C)` → running per-unit cost `C`. Operational concern: pool_state grows with one row per pool ever touched, even if currently empty. For most workloads this is fine (pool count is bounded by sku × location). Could be cleaned by a periodic vacuum process. Not a hot-path concern.

### 13.4 commit_group has no DB representation

The commit_group concept exists only in shmem. It groups multiple submissions for processing in one committer PG tx. After commit, the trx rows from a single commit_group all share the same `created_at` timestamp (within microseconds) and were written together, but there is no explicit commit_group_id column on trx. "What was committed together" is not a question the schema is built to answer — the queue layer can record this if needed (and it's lost on postmaster crash regardless), but the trx layer doesn't carry it.

If future audit needs require grouping (e.g., "show me all trxs that landed in the same committer batch"), a commit_group table can be added then.

### 13.5 Direct-path failure handling

When ledger_submit_trx encounters InsufficientInventory, UNIQUE constraint violation, or any other failure, it RAISEs EXCEPTION. PG aborts the caller's user-tx. No partial writes survive — the trx row, trx_line rows, pool_state mutations, and posting_line rows all roll back together because they were in the same tx. The caller sees the SQL exception.

This is the only failure mode for Path A. The caller's PG client decides what to do with the exception (retry, log, abort their own work).

### 13.6 Source_id type

source_id on trx and trx_line is BIGINT. Limits source tables to BIGINT primary keys. acct uses BIGINT throughout, so this works for the PoC. Future external integrations might require TEXT or UUID source_ids; defer until needed.

### 13.7 Account discovery for posting_lines

The ledger needs to know which accounts to debit/credit for each trx_type / line_type. Two approaches:

(a) Caller provides debit_account and credit_account in the SPI call. Simple, flexible, places the burden on the caller.

(b) Ledger has a rules table mapping (trx_type, line_type) → (debit_account, credit_account) for each (sku, location, dimensions). Caller doesn't specify; ledger looks up.

(a) is right for the PoC. (b) is a real production concern but separate from the cost-ledger PoC scope.

### 13.8 `pool_state.unit_cost` column-naming footgun

Per the per-method storage contract (§3.7), the column physically named `unit_cost` holds different semantics per method: a per-unit cost for layered methods (`fifo`/`lifo`/`specific`), and a cumulative `value_sum` for total-pool methods (`wac`/`wac_periodic`). The name fits the layered case; under WAC it stores a *total*, not a per-unit cost.

Ad-hoc SQL that reads `pool_state.unit_cost` without dispatching on `pool.method` produces wrong numbers for WAC pools. The correct read pattern:

```sql
SELECT pool_id,
       qty,
       CASE pool.method
         WHEN 'wac'           THEN unit_cost::float / NULLIF(qty, 0)
         WHEN 'wac_periodic'  THEN unit_cost::float / NULLIF(qty, 0)
         ELSE unit_cost::float
       END AS running_unit_cost
  FROM pool_state
  JOIN pool ON pool.id = pool_state.pool_id;
```

The PoC accepts this footgun in exchange for a minimal-touch implementation (no schema/hydration/bulk_write rewrite). A future productionization should either split into separate `value_sum` and `per_unit_cost` columns or rename the column to `unit_cost_or_value_sum`. See acct-h5gs commit history and the migration 0006 preamble for the cumulative-sum landing context; acct-mcey for the cross-path equivalence drift that motivated the storage shift.
