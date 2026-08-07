> **ARCHIVED — SUPERSEDED (2026-08-07).** Rescued from `poc/design_research/design-v3-abc.md`,
> where it existed only as an uncommitted working copy. This is the ledger-v3 PoC design
> that defined Paths A (direct) / B (routed) / C (provisional) over a shared schema.
> Path C won: alt-C was ratified 2026-08-07 (with drift-exposure bounds to be specified
> during hardening), and the surviving implementation + spec live on the ledger-v3.2 line
> (`poc/design_research/design-v3.2.md` and successors). Archived per convergence
> decision Q5 (2026-08-07). Historical record; do not edit.

# design-v3: PoC for cost-ledger architecture

## 1. Purpose

Establish the schema and three implementation paths for the cost-ledger, then measure all three under varying workloads to determine where each path wins.

Path A is direct: caller's user-tx does the ledger work inline via a pgrx SPI function. One PG transaction per caller submission. Lock contention is caller-to-caller. Layer-tracked methods (FIFO/LIFO/specific) compute strict in-order layer math on the hot path.

Path B is routed: caller stages a submission in shmem, returns immediately. Router (shmem) groups submissions by pool overlap. Committer pool (BGWorkers) processes commit_groups, each in one PG tx with batched writes. The trx row is created by the committer at successful processing time; existence of the trx row is the only durable signal that a submission was recorded. Layer-tracked methods still compute strict in-order layer math, but inside the committer rather than the caller's tx.

Path C is provisional: hot path (either direct-style or routed-style) records each trx with a provisional unit_cost computed from the pool's running aggregate. Layer-tracked methods do not touch per-layer state on the hot path; they update only the aggregate row, the same shape WAC uses. The trx_line stream is the durable record of what arrived and at what provisional cost. Turning those provisional costs into authoritative FIFO/LIFO costs — by walking the trx_line stream, running layer math, and posting cost-adjustment trxs for variance — is a recalc/close concern. **Recalc/close is out of scope for this PoC** (§13). The PoC measures the hot-path divergence only: whether the aggregate-only write pattern delivers the lock-hold reduction it promises versus Path A's strict in-order layer math.

This is the architectural pattern every major production ERP uses (SAP, Oracle, Dynamics — all decouple operational valuation from authoritative FIFO/LIFO reconciliation). Mid-period queries against trx_line.unit_cost return provisional costs; authoritative costs are derived post-recalc/close.

For WAC and STD methods, Path C is identical to Path A's hot path — there is nothing to reconcile. The architectural divergence is specifically for FIFO/LIFO/specific.

All three paths share the same schema, the same Rust transformation core, and the same correctness guarantees on what gets recorded. The differences are: when work happens, who holds locks, and whether mid-period costs are exact or provisional.

## 2. Schema

Greenfield. No migration from existing acct schema.

### 2.1 Enums

```sql
CREATE TYPE pool_method AS ENUM ('fifo','lifo','wac','std','specific');

CREATE TYPE pool_provisional_basis AS ENUM ('running_avg', 'standard');

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

`cost_adjustment` is NOT a trx_type, line_type, or posting_event_type in this PoC. It would be added by recalc/close (out of scope, §13) when that mechanism is implemented. ALTER TYPE ADD VALUE is a trivial migration.

### 2.2 Cost-ledger tables

```sql
CREATE TABLE pool (
    id                  BIGINT PRIMARY KEY,
    sku_id              BIGINT NOT NULL,
    location_id         BIGINT NOT NULL,
    identity_key        BIGINT NOT NULL DEFAULT 0,
    method              pool_method NOT NULL,
    provisional_basis   pool_provisional_basis NOT NULL DEFAULT 'running_avg',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (sku_id, location_id, identity_key)
);

CREATE TABLE standard_cost (
    sku_id       BIGINT NOT NULL,
    location_id  BIGINT NOT NULL,
    unit_cost    BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sku_id, location_id)
);
```

`provisional_basis` controls what the Path C hot path uses as the provisional unit_cost on depletions for FIFO/LIFO pools (see §6.2 and §14.10):
- `'running_avg'` (default): use the pool's aggregate row's unit_cost, maintained per WAC formula on every receipt. Self-contained — no external table lookup needed.
- `'standard'`: use the standard_cost table's unit_cost for (sku_id, location_id). Requires standard_cost to be populated for the pool's sku/location.

Only meaningful for FIFO/LIFO pools. Specific pools bypass Path C provisional mode entirely (§3.5 / §3.6) — they run strict layer math even on Path C, so the column is ignored. WAC and STD pools have no provisional/strict distinction — `provisional_basis` is ignored for these too.

`standard_cost` is keyed by (sku_id, location_id) — one current standard cost per sku/location pair. No effective-dating in the PoC; rows are updated in place when standards revise. Used by:
- STD pools (`pool.method = 'std'`): receipts/depletions both record at standard_cost.unit_cost; variance between actual and standard posts to a variance account on receipts (§3.4).
- FIFO/LIFO pools with `provisional_basis = 'standard'`: hot path uses standard_cost.unit_cost as the provisional cost for depletions, instead of the pool aggregate's running average.

If a STD pool or a 'standard'-basis pool is referenced and no standard_cost row exists for its (sku, location), RAISE EXCEPTION at hot-path time. Configuration error, fail loud.

pool carries no settlement watermarks. Path C's recalc/close (deferred, §13) would add what it needs when implemented — most likely a watermark or settled-state mechanism on trx_line. None of that is in PoC scope.

```sql

CREATE TABLE pool_state (
    pool_id            BIGINT NOT NULL REFERENCES pool(id),
    layer_id           BIGINT NOT NULL,
    qty                BIGINT NOT NULL,
    unit_cost          BIGINT NOT NULL,
    last_trx_line_id   BIGINT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pool_id, layer_id)
);
```

`layer_id` identifies the row's role within the pool:
- `layer_id = 0` is the **aggregate row**. Every pool has exactly one. Carries running qty and unit_cost (running average for WAC and for Path C provisional basis).
- `layer_id > 0` is a **materialized layer row** (FIFO/LIFO/specific under Paths A/B). The layer_id IS the receipt trx_line's id — i.e., `pool_state.layer_id = trx_line.id` for the receipt trx_line that created the layer. Ordering layers by layer_id approximates receipt-creation order (BIGSERIAL is monotonic in allocation order).

For WAC pools, only the aggregate row (layer_id=0) exists. For STD pools, no pool_state rows are needed. For layer-tracked pools under Paths A/B, both aggregate and layer rows exist. Under Path C, only the aggregate exists — layer rows would be materialized by recalc/close, deferred.

```sql

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
    source_trx_line_id  BIGINT REFERENCES trx_line(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX trx_line_trx ON trx_line (trx_id);
CREATE INDEX trx_line_pool ON trx_line (pool_id);
CREATE INDEX trx_line_source ON trx_line (source_trx_line_id) WHERE source_trx_line_id IS NOT NULL;
```

trx_line.id is BIGSERIAL — globally monotonic in allocation order. Ordering within a pool uses trx_line.id directly. The earlier v3 design used a per-pool `trx_seq` column, which was dropped because it required out-of-band sequence reservation under Path C and created unworkable lock-coordination problems. trx_line.posted_at could be denormalized from trx.posted_at if recalc/close (deferred) needs business-effective-time ordering rather than allocation-order; that's a deferred decision.

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

## 3. Method semantics

Each pool_method determines how trx_line rows interact with pool_state. The Rust core (ledger-core) implements one plan_apply per method.

### 3.0 Numeric representation and rounding

**Storage.** All monetary values (`unit_cost`, posting_line.amount, etc.) and quantities (`qty`) are stored as `BIGINT` with implicit fixed-point precision. The PoC uses **1 BIGINT unit = 1 micro-currency-unit (1e-6)** — i.e., one dollar is 1,000,000; one cent is 10,000; one mill (1/1000 of a cent) is 10; the smallest representable amount is 1 micro-cent (1e-6 of a currency unit). BIGINT range is ~9.2 × 10^18, so the representable range at this precision is ~±9.2 × 10^12 currency units (more than nine trillion dollars). Sufficient for any realistic ledger.

Production deployments wanting exact decimal arithmetic at arbitrary precision should swap BIGINT for PG's `NUMERIC(precision, scale)` type. This is a schema-change-and-recompile concern; the ledger-core arithmetic logic stays the same shape.

**Division and rounding.** Several operations produce non-integer intermediate values that must be coerced back to BIGINT:

- WAC weighted-average formula (§3.1): `new_unit_cost = (old_qty × old_unit_cost + Q × C) / new_qty`. Division.
- FIFO/LIFO depletion that spans multiple layers and records a blended unit_cost (§3.2): `applied_unit_cost = total_consumed_value / depletion_qty`. Division.
- Path C provisional cost under `'running_avg'` basis (§6.2): reads the WAC-rounded aggregate unit_cost. The rounding already happened upstream.

For all such divisions, ledger-core uses **banker's rounding** (round-half-to-even), not truncation or always-round-up. Rationale: under sustained workloads with many small fractional remainders, biased rounding accumulates a systematic drift in the pool's recorded value. Banker's rounding is symmetric — half-way cases round to the nearest even integer, which over a sufficiently random distribution of remainders cancels out to net-zero drift.

The Rust helper:

```rust
/// Banker's rounding (round-half-to-even) for integer division.
/// Returns the value of numerator / denominator rounded to nearest,
/// with exact-half cases rounding to the nearest even integer.
/// Panics if denominator == 0. Caller must guard.
pub fn banker_div(numerator: i64, denominator: i64) -> i64 {
    debug_assert!(denominator != 0, "banker_div: denominator must be non-zero");
    let q = numerator / denominator;
    let r = numerator % denominator;
    if r == 0 {
        return q;
    }
    // Compare 2*|r| to |d| without overflow.
    let abs_r = r.abs();
    let abs_d = denominator.abs();
    // Use the comparison 2*abs_r vs abs_d. For i64 inputs near the limits,
    // this could overflow; the PoC uses i128 for the comparison.
    let twice_r = (abs_r as i128) * 2;
    let abs_d_128 = abs_d as i128;
    let sign = if (numerator < 0) ^ (denominator < 0) { -1 } else { 1 };
    if twice_r < abs_d_128 {
        // Magnitude of remainder is less than half: round toward zero.
        q
    } else if twice_r > abs_d_128 {
        // Magnitude greater than half: round away from zero.
        q + sign
    } else {
        // Exactly half: round to nearest even.
        if q % 2 == 0 { q } else { q + sign }
    }
}
```

**Bounded per-trx loss.** Even with banker's rounding, individual divisions still produce a residual of up to 0.5 LSB (half a micro-cent). That residual is lost — the ledger doesn't carry forward fractional precision below the LSB. For most workloads this is invisible (a single division loses at most 5e-7 of a currency unit). For workloads with many sequential divisions on small running balances (e.g., a pool that repeatedly fills and depletes with non-divisor-friendly numbers), the cumulative loss across many ops is bounded by the number of ops × 0.5 LSB. At 10^9 ops, worst-case cumulative loss is 0.5 currency units — still trivial relative to typical ledger balances.

**Bigger losses where rounding compounds (out of PoC scope).** A more rigorous approach would carry an explicit residual column on pool_state and accumulate fractional remainders there, posting them as penny-rounding variances when they cross integer thresholds. That's a recalc/close design decision (the same place where variance handling lives generally). For the PoC: accept the bounded loss, document it, measure it via reconciliation checks (sum of posting_line amounts should equal sum of (trx_line.qty × trx_line.unit_cost) within bounded drift).

### 3.1 WAC

pool_state has exactly one row per pool, at layer_id = 0 (the aggregate row). It carries the pool's total qty and the running average unit_cost.

**Receipt** of qty Q at unit_cost C:
- INSERT trx_line (qty=Q, unit_cost=C). trx_line.id auto-assigned by BIGSERIAL.
- UPSERT pool_state at layer_id=0:
  - If row doesn't exist: insert with qty=Q, unit_cost=C.
  - Otherwise: new_qty = old_qty + Q. Then:
    - If new_qty > 0: new_unit_cost = banker_div(old_qty × old_unit_cost + Q × C, new_qty). Standard weighted-average formula, with banker's rounding on the division per §3.0.
    - If new_qty <= 0: preserve old_unit_cost (cannot compute a meaningful average into an unreplenished short position; the previous average serves as the basis for any subsequent receipt that brings the pool back positive). This guard handles both the qty=0 case directly and the negative-qty oversold case.
- The formula-step guard (new_qty <= 0 path) is load-bearing. Naive implementation of the weighted-average formula divides by new_qty without checking, panicking the Rust core or raising a SQL exception when new_qty = 0. The guard must run before the division.

**Zero-crossing replenishment** (e.g., old_qty=-5, Q=15, new_qty=10): the formula above handles this correctly. The pool's stored value (qty × unit_cost) transitions from -5 × old_unit_cost to 10 × new_unit_cost where new_unit_cost = banker_div(-5 × old_unit_cost + 15 × C, 10). This is the standard single-pool WAC treatment — temporary deficit valued at old_unit_cost, receipt at C, blended over the resulting positive balance. Split-receipt GL accounting (booking the -5-to-0 backfill at old_unit_cost against COGS, and 0-to-10 accumulation at C against inventory) is a GL-treatment concern out of PoC scope.

**Depletion** of qty Q:
- Read pool_state at layer_id=0. If qty < Q, error InsufficientInventory.
- applied_unit_cost = current pool_state.unit_cost.
- INSERT trx_line (qty=-Q, unit_cost=applied_unit_cost).
- UPDATE pool_state at layer_id=0: new_qty = qty - Q. unit_cost unchanged (avg only changes on receipts, by definition of WAC).

### 3.2 FIFO

pool_state has one aggregate row at layer_id = 0 (tracking running qty/unit_cost at the pool level under WAC formula) plus one row per live receipt layer at layer_id = trx_line.id of the receipt. Layer rows DELETE when qty reaches zero.

**Receipt** of qty Q at unit_cost C:
- INSERT trx_line (qty=Q, unit_cost=C). trx_line.id auto-assigned by BIGSERIAL.
- INSERT pool_state (layer_id=trx_line.id, qty=Q, unit_cost=C, last_trx_line_id=trx_line.id).
- UPDATE pool_state at layer_id=0: aggregate qty + running average per WAC formula (the aggregate row is maintained alongside layers for query convenience).

**Depletion** of qty Q:
- Read layer rows: `SELECT layer_id, qty, unit_cost FROM pool_state WHERE pool_id=$1 AND layer_id > 0 ORDER BY layer_id ASC`. Take layers until accumulated qty >= Q.
- For each layer touched, INSERT trx_line:
  - qty = -consumed_from_this_layer
  - unit_cost = layer.unit_cost
  - source_trx_line_id = layer.layer_id (= the receipt's trx_line.id)
- For each layer touched, UPDATE pool_state.qty -= consumed (or DELETE the layer row if qty hits zero).
- UPDATE pool_state at layer_id=0: aggregate qty -= Q.

### 3.3 LIFO

Same as FIFO but ORDER BY layer_id DESC.

### 3.4 STD

No pool_state rows. Standard costs live in the `standard_cost` table (§2.2), keyed by (sku_id, location_id).

**Receipt** of qty Q at actual unit_cost C_actual:
- Look up standard_cost.unit_cost = C_std for the pool's (sku_id, location_id). If no row exists, RAISE EXCEPTION.
- INSERT trx_line with unit_cost = C_std (the standard cost is what gets recorded as the line's cost).
- INSERT posting_lines:
  - Inventory account: debit Q × C_std.
  - Source account (AP, WIP, etc.): credit Q × C_actual.
  - Variance account: debit/credit the difference Q × (C_actual - C_std) — the purchase price variance.

**Depletion** of qty Q:
- Look up standard_cost.unit_cost = C_std.
- INSERT trx_line with unit_cost = C_std.
- INSERT posting_lines per the depletion's debit/credit accounts at Q × C_std.

STD pools have no layer state because the unit_cost is a single number per (sku, location), not per-receipt. The aggregate pool_state row could still be maintained for qty tracking (running on-hand count); the unit_cost on the aggregate row, if maintained, just mirrors standard_cost.unit_cost. PoC implementation may choose to skip pool_state for STD entirely and derive on-hand qty from trx_line sums when needed.

When standard_cost is revised, the new value applies to future trx_lines. Already-recorded trx_lines retain the C_std they were recorded with (revaluation of existing inventory is a separate `revaluation_run` trx_type).

### 3.5 Specific-id

Each unit is its own pool (pool.identity_key = unit_id, pool.method = 'specific'). The pool has one layer with qty=1 from its receipt; depletion of that unit consumes the entire layer (qty becomes 0, row DELETEd). Same shape as FIFO with K=1.

**Specific-id pools always use strict mode, even on Path C.** Provisional mode exists to avoid layer iteration on layer-tracked methods; with K=1 there's only one layer to read, and the choice of which layer to consume is uniquely determined by the caller-provided identity_key (not by FIFO/LIFO ordering). The lock-hold reduction Path C exists to deliver doesn't apply — there's nothing to defer. The hot path under Path C for a specific pool runs identical SQL to Path A: read the one layer, deplete it, INSERT trx_line with source_trx_line_id pointing at the receipt, DELETE the layer row. pool.provisional_basis is ignored for specific pools.

### 3.6 Provisional cost mode (Path C divergence)

The semantics in §3.1-§3.5 describe **strict mode** — the cost computed on the hot path is the final, authoritative cost. Paths A and B always use strict mode. Path C uses strict mode for WAC, STD, and specific pools (see §3.4 and §3.5 — there's nothing to defer for these methods on Path C).

Path C introduces **provisional mode** for FIFO and LIFO pools only. Under provisional mode:

- **Receipts** behave identically to WAC's running-aggregate update. The hot path writes only the layer_id=0 row of pool_state (the aggregate: qty, running average unit_cost). No per-layer pool_state rows are created on the hot path. The receipt's trx_line still records the actual receipt's qty and unit_cost (that's the source of truth for what arrived); the layer history is recoverable by scanning trx_line.
- **Depletions** compute applied_unit_cost from one of two sources, controlled by `pool.provisional_basis` (§2.2):
  - `'running_avg'` (default): use pool_state.layer_id=0.unit_cost — the running average maintained per the WAC formula.
  - `'standard'`: use standard_cost.unit_cost for the pool's (sku_id, location_id) — a standing standard cost maintained externally.
  The chosen value gets recorded as the depletion trx_line's unit_cost. `source_trx_line_id` is left NULL — the hot path does not commit to which historical receipt is being consumed; that determination happens during recalc/close.
- **Posting_lines** for depletions on the hot path book the provisional amount (qty × provisional_unit_cost) against the configured debit/credit accounts.

Recalc/close (deferred, §13) is the mechanism that later turns provisional costs into authoritative ones. It walks the trx_line stream per pool, runs strict FIFO/LIFO layer math, and posts cost-adjustment trxs for any variance against the provisional costs. The PoC does not implement recalc/close — it measures only the hot-path properties under provisional mode.

The hot path under provisional mode for a FIFO/LIFO pool produces the same SQL footprint as WAC under strict mode: one row update on pool_state at layer_id=0 (aggregate qty decrement, plus running-average recompute on receipts), no per-layer iteration, lock-hold time bounded by aggregate update rather than by number of layers consumed. **This is what the PoC measures.** For `'standard'` basis, the hot path additionally reads standard_cost (one constant-time index seek) but does not write it.

Both provisional_basis choices produce identical lock-hold characteristics; the basis choice affects only the magnitude of the variance that recalc/close (deferred) would later correct, not the hot-path performance profile.

## 4. Path A: direct write

Path A always uses **strict mode** semantics from §3.1-§3.5. For layer-tracked pools, this means the hot path acquires pool_lock, iterates layers in order, writes per-layer pool_state mutations, and computes exact FIFO/LIFO costs at commit time. Mid-period reads see authoritative costs because every depletion is settled before the caller's tx commits.

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
   SELECT pool_id, layer_id, qty, unit_cost, last_trx_line_id
     FROM pool_state
    WHERE pool_id = ANY($1::bigint[])
    ORDER BY pool_id, layer_id
   ```
   ORDER BY is explicit so layer-ordered methods (FIFO ASC, LIFO DESC) can rely on the layer ordering when ledger-core demultiplexes rows per pool. The PRIMARY KEY (pool_id, layer_id) on pool_state makes the sorted scan free — it's an index-ordered traversal. ledger-core additionally re-sorts per-pool defensively before passing the snapshot to plan_apply; the database-side ORDER BY is the contract, the client-side sort is the enforcement.
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

## 5. Path B: routed

Path B always uses **strict mode** semantics from §3.1-§3.5. The committer (not the caller) performs the layer iteration and writes the per-layer pool_state mutations, but the math is the same as Path A's. Mid-period reads see authoritative costs once a commit_group's tx commits.

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

5. **Pre-flight dedup against trx.** Bulk-read existing trx rows matching the batch's (trx_type, source_id) pairs:
   ```sql
   SELECT trx_type, source_id FROM trx
    WHERE (trx_type, source_id) IN (... batch's pairs ...);
   ```
   Any submission whose (trx_type, source_id) is already in trx is a duplicate of a previously-recorded receipt. Exclude these submissions from the commit_group immediately — do not pass them to plan_apply, do not acquire locks on their pools. The committer logs the duplicate (caller-facing observability isn't in PoC scope) and proceeds with the remaining submissions.

   Rationale: the trx table's UNIQUE (trx_type, source_id) constraint will fire at INSERT time in step 10 if any duplicate slips through, but a UNIQUE violation aborts the ENTIRE committer tx — losing the work for all other submissions in the commit_group. Recovery would require parsing PG error detail strings to identify which submission caused the conflict, then pristine-replaying without it. Pre-flight dedup is cheap (one SELECT), reliable (no error-string parsing), and composes naturally with pristine-replay: duplicates are excluded BEFORE plan_apply, so they never enter the working snapshot. The UNIQUE constraint remains as a structural backstop, but the committer no longer depends on it firing for happy-path control flow.

6. Compute the union of pool_ids across all included submissions (after dedup). Sort ascending, dedup.
7. Acquire pool_lock FOR UPDATE in singleton-loop order. Lazy-create with INSERT ON CONFLICT as needed.
8. Bulk-read pool_state for all touched pools:
   ```sql
   SELECT pool_id, layer_id, qty, unit_cost, last_trx_line_id
     FROM pool_state
    WHERE pool_id = ANY($1::bigint[])
    ORDER BY pool_id, layer_id
   ```
   Same query and ordering contract as Path A §4.2 step 4. The PRIMARY KEY (pool_id, layer_id) makes the sort an index-ordered traversal. ledger-core demultiplexes per-pool and re-sorts defensively before plan_apply.
9. For each submission, dispatch to ledger-core::plan_apply with the snapshot. Apply results to a working snapshot. On plan_apply failure for a submission: exclude that submission (pristine-snapshot replay semantics), restart from pristine snapshot with the excluded set. Excluded submissions produce no trx row.
10. After clean pass: bulk INSERT trx (one row per included submission), bulk INSERT trx_line, bulk INSERT/UPSERT/UPDATE/DELETE pool_state, bulk INSERT posting_line. UNIQUE (trx_type, source_id) is a backstop: in normal operation duplicates were already filtered in step 5 and a violation at this step is a bug, not an expected condition.
11. COMMIT. One fsync.
12. Cleanup: CAS staging entries routed → empty, free arena blocks. Three-case CAS handling per v2.1 §7.8 (success, ejected entries left at pending, router-died-mid-stamp entries cleaned via CAS 2→0).

One fsync per commit_group. With N submissions per commit_group, fsync cost amortizes by N. Each committed submission produces exactly one trx row; failed submissions produce nothing.

### 5.5 Recovery

- **Router death**: shmem boot sweep on router restart. Staging entries at processing (valid=2) get inspected: if their CommitterQueueEntry doesn't exist or is at empty, revert to pending; clear superbatch_id before CAS to avoid stale-value resurrection.
- **Committer death**: CommitterIdentityRegistry (extension-owned shmem array) tracks active committers via (slot, token). Liveness check is registry lookup. Dead committer's in-flight commit_group is reclaimed by next active committer via CAS on the queue entry. Whether the dead committer's PG tx committed is checked by looking up its xid in pg_xact:
  - Committed: the trx rows exist; the recovery committer doesn't reprocess; releases the shmem entries.
  - Aborted (or never started): the trx rows don't exist; the recovery committer reprocesses the commit_group normally.
- **Postmaster crash**: shmem is gone. All in-flight submissions are lost. No trx rows exist for them (they were never written). No DB cleanup is needed — the only thing in the DB are trx rows for previously-committed submissions, and those are durable and correct. Callers whose submissions were in flight at crash time observe the loss (their polling never sees a trx for that source) and resubmit.

### 5.6 Per-commit_group SPI count

For a commit_group with N submissions touching P deduped pools:

- 1 pre-flight dedup SELECT against trx.
- P singleton FOR UPDATE on pool_lock + P lazy INSERT ON CONFLICT.
- 1 bulk pool_state read.
- ~7 bulk writes: trx INSERT, trx_line INSERT, pool_state INSERT (receipts), pool_state UPSERT (WAC), pool_state UPDATE (partial depletions), pool_state DELETE (consumed layers), posting_line INSERT.

Total: ~9 bulk + 2P singleton. For N=50 submissions at P=50 deduped pools: ~109 SPI calls per commit_group, amortizing to ~2.18 SPI/submission. For N=10 at P=10: ~29 SPI calls per commit_group, ~2.9 SPI/submission.

Compare Path A at ~7-17 SPI per trx. Path B amortizes by batching.

## 6. Path C: provisional hot path

Path C decouples hot-path recording from authoritative FIFO/LIFO reconciliation. The hot path records each trx with a provisional unit_cost computed from the pool's running aggregate, performs no per-layer state mutations, and returns. The trx_line stream is the durable record of what arrived.

Turning those provisional costs into authoritative FIFO/LIFO ones is recalc/close (deferred, §13). This document scopes Path C as: the hot-path divergence, what the PoC measures, and a sketch of how recalc/close could be implemented later.

For WAC and STD pools, there is nothing to reconcile; Path C's hot path produces the final cost directly. The architectural divergence applies specifically to FIFO, LIFO, and specific.

This pattern is what every major production ERP does. SAP S/4HANA operationally values inventory at Standard Price (Price Control S) and runs Actual Costing (CKMLCP) at period-end to revalue consumption. Oracle Fusion Cost Management runs a continuous background Cost Processor. Dynamics 365 F&O records issues at a Running Average Cost Price and runs Inventory Close at period-end. All three vendors independently arrived at the conclusion that strict in-order FIFO/LIFO on the hot path does not scale.

### 6.1 SPI surface

Path C exposes two flavors of hot-path entry, corresponding to direct and routed shapes:

```rust
ledger_submit_trx_c(
    trx_type: trx_type,
    source_id: BIGINT,
    posted_at: TIMESTAMPTZ,
    lines: ARRAY of (line_type, source_id, pool_id, qty, unit_cost, debit_account, credit_account)
) RETURNS BIGINT  -- trx.id (direct flavor; like ledger_submit_trx but provisional for layer-tracked pools)

ledger_enqueue_trx_c(
    trx_type: trx_type,
    source_id: BIGINT,
    posted_at: TIMESTAMPTZ,
    lines: ARRAY of (line_type, source_id, pool_id, qty, unit_cost, debit_account, credit_account)
) RETURNS BIGINT  -- submission_id (routed flavor; like ledger_enqueue_trx but provisional)
```

For the PoC, both flavors are in scope. Direct Path C (§6.2) demonstrates per-caller-tx provisional cost recording with reduced lock-hold time vs Path A. Routed Path C (§6.3) demonstrates the same provisional cost handling under Path B's batching architecture — the combination that the hot-pool deep-FIFO regime needs. The two flavors map onto the same four-way matrix that Path A vs Path B does (low/high concurrency × disjoint/overlapping pools); Path C's value at the architecturally-interesting cell (high concurrency, hot FIFO/LIFO pools) is fully realized only with routed.

### 6.2 Hot-path function logic (direct flavor)

1. Allocate trx.id.
2. Compute the set of pool_ids touched. Sort ascending, dedup.
3. Acquire locks in singleton-loop sorted order on pool_lock; lazy-create as needed.
4. Bulk-read per-pool routing info and aggregate state for all touched pools:
   ```sql
   SELECT p.id AS pool_id,
          p.method,
          p.provisional_basis,
          ps.qty,
          ps.unit_cost
     FROM pool p
     LEFT JOIN pool_state ps ON ps.pool_id = p.id AND ps.layer_id = 0
    WHERE p.id = ANY($1::bigint[])
    ORDER BY p.id
   ```
   LEFT JOIN because a brand-new pool may not yet have a pool_state aggregate row (created lazily on first receipt). The aggregate's qty/unit_cost are NULL for an unwritten pool; ledger-core treats NULL as "fresh, no prior state."

5. For any pool with `method = 'std'` OR `provisional_basis = 'standard'`, bulk-read standard_cost:
   ```sql
   SELECT p.id AS pool_id, sc.unit_cost AS standard_unit_cost
     FROM pool p
     JOIN standard_cost sc ON sc.sku_id = p.sku_id AND sc.location_id = p.location_id
    WHERE p.id = ANY($2::bigint[])  -- subset that needs standard cost
   ```
   If a pool in the subset has no matching standard_cost row, RAISE EXCEPTION (configuration error).

6. For any pool with `method IN ('fifo','lifo','specific')` (where layer state matters), bulk-read layer rows:
   ```sql
   SELECT pool_id, layer_id, qty, unit_cost, last_trx_line_id
     FROM pool_state
    WHERE pool_id = ANY($3::bigint[]) AND layer_id > 0  -- subset of specific pools that need layer access
    ORDER BY pool_id, layer_id
   ```
   For FIFO/LIFO under provisional mode, no layer rows exist on the hot path — this query returns nothing for those pools. For specific pools (which use strict mode even on Path C, §3.5), this returns the one layer row per pool.

7. Call ledger-core dispatching per pool's method:
   - `method = 'wac'`: plan_apply_wac (strict; identical to Path A).
   - `method = 'std'`: plan_apply_std using standard_unit_cost looked up in step 5; receipts emit variance posting_lines per §3.4.
   - `method = 'specific'`: plan_apply_specific (strict; identical to Path A; consumes the one layer row).
   - `method = 'fifo'` or `'lifo'`: plan_apply_provisional. Receipts update aggregate per the §3.1 WAC formula (with the §3.1 divide-by-zero guard and §3.0 banker's rounding on the division). Depletions get applied_unit_cost from either `aggregate.unit_cost` (if provisional_basis = 'running_avg') or `standard_unit_cost` (if provisional_basis = 'standard'). new_qty = old_qty - Q on aggregate. trx_line.source_trx_line_id = NULL.

8. On error (insufficient inventory for any pool — checked against aggregate.qty for WAC/FIFO/LIFO provisional, against the one layer for specific, against no inventory for STD since STD pools don't track on-hand qty in pool_state): RAISE EXCEPTION, caller's tx aborts.

9. Apply PlanResult:
   - INSERT trx.
   - Bulk INSERT trx_line rows.
   - Bulk UPSERT pool_state (aggregate row updates for all methods that maintain it; layer row mutations for specific pools).
   - Bulk INSERT posting_line rows.
10. Return trx.id.

Caller's user-tx commits. One fsync.

**Lock-hold properties.** For FIFO/LIFO pools under provisional mode, lock-hold time per trx is bounded by aggregate-row work only — no layer iteration. For a hot SKU under FIFO with hundreds of historical layers, Path C holds the lock for the same duration as a WAC update — orders of magnitude less than Path A would. This is what the PoC measures. For WAC, STD, and specific pools, Path C's lock-hold characteristics match Path A's (specific has K=1 so iteration cost is constant; WAC has no layers; STD has no pool_state mutation at all).

Per-trx SPI count for Path C direct, dominant case (FIFO/LIFO pool, P deduped pools):
- 1 INSERT trx.
- P singleton FOR UPDATE on pool_lock + P lazy ON CONFLICT.
- 1 SELECT pool + pool_state aggregate join.
- 0-1 SELECT standard_cost (only if any 'standard'-basis or STD pools are touched).
- 0-1 SELECT pool_state layer rows (only if any specific pools are touched).
- 1 bulk INSERT trx_line.
- 1 bulk UPSERT pool_state.
- 1 bulk INSERT posting_line.

Total: ~5-7 SPI + 2P singleton. For P=1: ~7-9 SPI. Same count as Path A; the throughput difference comes from reduced lock-hold time on FIFO/LIFO depletions, not reduced SPI count.

### 6.3 Routed flavor (ledger_enqueue_trx_c)

Routed Path C reuses Path B's queue/router/committer architecture (§5) with the inner work swapped for provisional-mode semantics. This subsection enumerates only the differences from Path B.

**Reused from Path B unchanged:**
- Shmem staging layout (§5.2): atomically claimed entries via CAS; routed → pending → in_progress lifecycle; arena allocator for variable-size submission payloads.
- Router (§5.3): pool-overlap-based grouping into commit_groups. Single-writer router worker.
- Committer pool (§5.4 steps 1-8): claim commit_group, open tx, pg_xact_status check on caller user_tx_xids, pre-flight dedup against trx, pool union and sorted lock acquisition, bulk hydration of pool state and (for STD pools or 'standard'-basis FIFO/LIFO pools) standard_cost.
- Recovery (§5.5): orphan commit_group reclaim on committer death; trx UNIQUE constraint guards against double-write; shmem boot sweep on postmaster restart.

**Different from Path B:**

1. **Inner work uses plan_apply_provisional dispatch (§6.2 step 7), not plan_apply.** The committer's per-submission work for FIFO/LIFO pools does NOT iterate layers, does NOT compute strict in-order layer math, does NOT touch pool_state at layer_id > 0. It updates only the aggregate row, recording provisional costs. For WAC, STD, and specific pools, behavior is identical to Path B (those methods run strict semantics under Path C too — see §3.4 / §3.5 / §3.6).

2. **No pristine-snapshot replay.** Per §14.9, Path C has no cross-trx state dependency on the hot path. Each submission's plan_apply_provisional reads the aggregate, applies its delta, contributes to a working snapshot. A submission that fails (e.g., depletion exceeds aggregate qty) is excluded from the commit_group; remaining submissions proceed using the working snapshot as-is. **No pristine restart needed.** The aggregate's qty/unit_cost are commutative under the operations the committer performs:
   - For receipts: `total_value += Q × C` and `qty += Q` are both associative-commutative additions.
   - For depletions under `running_avg` basis: `qty -= Q` is a commutative decrement. The unit_cost read for the provisional cost is whatever the working snapshot has at the moment the submission is processed; the recorded provisional cost is "the running average given the batch's processing order" rather than "the chronologically correct cost," which is the same property as direct flavor and is corrected by recalc/close (deferred, §6.4).
   - For depletions under `standard` basis: applied_unit_cost comes from standard_cost (read once at hydration), not from the running aggregate. `qty -= Q` is the only aggregate mutation. Fully order-independent within the batch.

   Failed submissions are simply removed from the working snapshot's contribution list. Remaining writes proceed.

3. **Batching benefit on hot pools.** The architectural win Path C routed provides over Path C direct: 1000 concurrent submissions to one hot FIFO pool become one commit_group, processed in one PG tx, with one pool_lock acquisition and one aggregate UPDATE encompassing the batch's combined delta. Direct flavor would serialize 1000 individual pool_lock acquisitions and 1000 individual aggregate UPDATEs. Routed flavor collapses both costs into a single round.

   For `standard`-basis FIFO/LIFO pools, the batching is even cleaner — standard_cost is read once at hydration, every depletion in the batch uses it, no within-batch state evolution. Pure data shuffle.

4. **SPI count per commit_group.** Same shape as Path B (§5.6): 1 INSERT trx (per submission), 1 bulk INSERT trx_line, 1 bulk UPSERT pool_state (aggregate rows only), 1 bulk INSERT posting_line. Plus the hydration reads: 1 SELECT pool+aggregate, 1 SELECT standard_cost (if any 'standard'-basis or STD pools in batch), 1 SELECT layer rows (if any specific pools in batch). Plus P singleton pool_lock acquisitions + P lazy creates + 1 pre-flight dedup SELECT.

   Total: ~9 bulk + 2P singleton, comparable to Path B's ~9 + 2P. The difference is what happens inside plan_apply_provisional (constant-time aggregate work vs Path B's layer iteration). At deep-pool FIFO scale, the inner-loop time difference dominates SPI count.

5. **Per-submission failure isolation.** Path B uses pristine-replay precisely because excluded trxs would have left stale intermediate state. Path C routed needs no such mechanism — a failed submission contributes nothing to the working snapshot, remaining submissions continue using the snapshot as if the failed submission never arrived. Simpler, faster failure recovery within a commit_group.

The committer's plan_apply_provisional dispatch is also where the basis decision (per-pool `provisional_basis`) is applied — same logic as direct flavor (§6.2 step 7).

### 6.4 Recalc / close (deferred)

The PoC does not implement the mechanism that turns provisional FIFO/LIFO/specific costs into authoritative ones. Authoritative cost reconciliation — call it recalc, settlement, or period close depending on the model — is out of scope. See §13.

What it would do, in broad strokes: walk the trx_line stream for each layer-tracked pool in chronological order (some ordering choice — allocation order via trx_line.id, business-effective time via JOIN to trx.posted_at, or a derived Cost Date), run strict FIFO/LIFO layer math, and post cost-adjustment trxs and trx_lines for any variance between the recomputed authoritative cost and the provisional cost recorded on the hot path. Materialized layer rows under pool_state at layer_id > 0 would also be written.

Implementation options that production ERPs use:

- **Oracle-style continuous worker**: a background worker pool runs a per-pool replay continuously. Variance is posted as soon as it's detected. Mid-period queries see provisional costs until the worker catches up.
- **SAP-style on-demand**: caller invokes `ledger_settle_pool(pool_id)` before a statutory query. Forces synchronous reconciliation.
- **Dynamics-style periodic close**: an Inventory Close job runs on schedule (nightly, end-of-period), reconciles everything, gates the period transition on completion.

Each has different tradeoffs (lag, resource consumption, predictability) and different schema requirements: the continuous worker needs progress tracking (a watermark on pool, or a settled-state column on trx_line, or full-recompute idempotency); the on-demand mode needs an SPI entrypoint; the periodic close needs the accounting_period table's close hook.

These are concrete next-step decisions, not v3 PoC decisions. The hot path's behavior is independent of which reconciliation model is chosen — provisional costs are recorded the same way regardless. The PoC validates the hot path; the reconciliation mechanism is chosen and built later based on what the production workload actually demands.

When recalc/close is added, the schema additions are likely to include: a `cost_adjustment` value in the trx_type / line_type / posting_event_type enums; a `cost_adjustment_id_seq` sequence for unique trx.source_id on adjustment trxs; possibly a denormalized `trx_line.posted_at` for business-time ordering; possibly a `total_value` column on pool_state for variance-into-empty-pool absorption; possibly watermark columns on pool or a `settlement_state` column on trx_line. None of those columns or enum values are in the v3 schema — they get added when recalc/close is built.

## 7. ledger-core (shared Rust crate)

```
ledger-core/
  src/
    method.rs                  - PoolMethod enum, plan_apply trait
    fifo.rs                    - FIFO plan_apply (strict)
    lifo.rs                    - LIFO plan_apply (strict)
    wac.rs                     - WAC plan_apply (uses numeric::banker_div on receipts)
    std.rs                     - STD plan_apply (with variance)
    specific.rs                - Specific-id plan_apply (strict)
    provisional.rs             - plan_apply_provisional for FIFO/LIFO
                                 (aggregate-only, NULL source_trx_line_id);
                                 dispatches to wac.rs for WAC, std.rs for STD,
                                 specific.rs for specific
    numeric.rs                 - banker_div (round-half-to-even integer division)
                                 + precision constants
    snapshot.rs                - Snapshot type
                                 (HashMap<pool_id, PoolStateRows>)
    plan.rs                    - PlanResult type
                                 (trx_line, pool_state mutations, posting_line)
    error.rs                   - LedgerError enum
                                 (InsufficientInventory, MethodMismatch, etc.)
```

Pure Rust. No pgrx dependency. Unit-testable in isolation.

The three pgrx extensions (ledger-direct for Path A, ledger-routed for Path B, ledger-direct-c for Path C) all depend on ledger-core. They invoke its functions with the same Snapshot types and consume the same PlanResult outputs. Differences are in which entry point they call:

- **Path A**: ledger-core::plan_apply (strict, method-dispatched).
- **Path B**: ledger-core::plan_apply (strict, method-dispatched), invoked by the committer.
- **Path C hot path**: ledger-core::plan_apply_provisional (aggregate-only for layer-tracked methods; identical to plan_apply for WAC and STD).

A settle/recalc entry point would be added by recalc/close (deferred, §13).

## 8. Pristine-snapshot replay

Carried forward from v2.1 §7.6. Applies to Paths A and B.

When a trx's plan_apply errors during the working snapshot's evolution, the committer (or direct-path SPI function) does NOT delta-rollback. It:

1. Adds the failing trx to the excluded set.
2. Discards the working snapshot.
3. Clones the pristine (post-hydration, pre-plan_apply) snapshot.
4. Re-runs plan_apply for all non-excluded trxs in chronological order.
5. Repeats until a clean pass or all trxs excluded.

Naive delta-rollback corrupts AVG/FIFO state when subsequent trxs have already booked against the now-reverted intermediate state. Pristine-replay handles this correctly at the cost of O(replay_passes × events) work in the worst case. In practice 0 or 1 replay passes per commit_group.

For Path A with N=1 trx per call, this collapses: a failing trx just aborts the caller's user-tx. No replay needed. Pristine-replay only matters in Path B where commit_groups have N > 1.

**Path C does not need pristine-replay.** Each trx on the Path C hot path updates only the aggregate row and produces only its own trx_line/posting_line rows. There is no cross-trx state dependency on the hot path (every trx reads the same starting aggregate, applies its own delta, writes its own trx_line). A failure in one trx within a batched submission (if routed-flavor Path C is ever implemented) doesn't corrupt other trxs because the aggregate update can be cleanly skipped for the failing trx. Recalc/close (deferred, §13) has its own failure semantics that are out of scope here.

## 9. Testing strategy

### 9.1 ledger-core unit tests

For each method (FIFO, LIFO, WAC, STD, specific), drive synthetic snapshots through plan_apply and assert PlanResult correctness:

- Single receipt into empty pool.
- Single depletion of available pool.
- Depletion exceeding available qty (InsufficientInventory).
- FIFO: depletion spanning multiple layers.
- WAC: divide-by-zero guard when depleting to qty=0 then receiving.
- WAC: avg calculation across mixed-cost receipts.
- WAC: zero-crossing replenishment (old_qty negative, receipt brings new_qty positive).
- Idempotent replay (same input → same output).

For numeric::banker_div (§3.0):
- Exact division (no remainder): returns the quotient unchanged.
- Below-half remainder: rounds toward zero.
- Above-half remainder: rounds away from zero (toward +inf for positive, -inf for negative).
- Exactly-half cases: rounds to nearest even — verify both `q even → q` and `q odd → q ± 1` cases.
- Negative numerator, positive denominator: same rounding rules with sign tracking.
- Both signs negative: same.
- Bias check: sum of banker_div(n, 2) for n in 0..1000 differs from sum of (n/2) by at most O(1), demonstrating bias cancellation vs always-truncate.

For plan_apply_provisional (Path C hot path):
- For WAC and STD inputs: results identical to plan_apply.
- For FIFO/LIFO inputs: receipts update aggregate per WAC formula (with banker_div rounding); depletions use aggregate's running unit_cost (for 'running_avg' basis) or standard_cost (for 'standard' basis) as applied_unit_cost; source_trx_line_id NULL.
- For specific inputs: identical to plan_apply (specific bypasses provisional mode, §3.5).

Recalc/close logic (settle) is deferred per §6.4; no tests in PoC scope.

### 9.2 ledger-direct integration tests

Run against a real PostgreSQL with the schema installed. Test cases:

- Submit one trx, verify trx + trx_line + pool_state + posting_line all written and consistent.
- Submit FIFO receipt + depletion, verify layer math.
- Submit WAC receipts + depletions, verify running average.
- Insufficient inventory: verify error propagates as a SQL exception, caller's user-tx aborts, no trx or trx_line or pool_state rows are written.
- Duplicate submission: same (trx_type, source_id) submitted twice; second submission fails on UNIQUE constraint; caller's second tx aborts; first trx is intact.
- Concurrent callers (separate psql sessions) submitting to overlapping pools: verify serialization, no deadlocks, consistent final state.
- Concurrent callers to disjoint pools: verify true parallelism (no false serialization).

### 9.3 ledger-routed integration tests

Same as 9.2 plus:

- Submit, verify submission accepted; poll for trx existence by (trx_type, source_id); confirm trx appears after processing.
- Concurrent submissions to overlapping pools: verify affinity grouping (one commit_group handles all overlap, single committer per group).
- Committer death mid-processing: verify orphan recovery picks up the commit_group; trx rows for submissions in the group either all exist (recovery committer's tx committed) or all don't (recovery committer reprocesses fresh).
- Postmaster restart with in-flight submissions: verify trx rows from prior commits are intact; in-flight submissions are lost (no DB orphans); the system accepts new submissions and processes normally.
- Eject cycling: a caller's user-tx held open for several seconds; verify committer doesn't stall, eject_count incremented, eventually caller commits or wall-clock fires.
- Duplicate submission: same (trx_type, source_id) submitted twice. Test variants: (a) both arrive in the same commit_group — pre-flight dedup (§5.4 step 5) catches both as duplicates of each other (the SELECT against trx returns zero rows the first time both are seen, but a UNIQUE check within the commit_group's own batch should also catch the in-batch duplication; for the PoC, both are processed if not already in trx, and the bulk INSERT at step 10 will fire UNIQUE for the second — fall back to pristine-replay); (b) the second arrives after the first has committed — pre-flight dedup catches it, second is excluded, no UNIQUE fired at commit. Verify no UNIQUE violations escape to abort the committer's tx in case (b).

### 9.4 Path C integration tests

**ledger-direct-c (direct flavor) hot-path tests:**

- Submit FIFO receipt + depletion via ledger_submit_trx_c, verify aggregate updated correctly, trx_line records provisional cost, no layer_id > 0 pool_state rows created.
- Submit WAC receipt + depletion via ledger_submit_trx_c, verify behavior identical to ledger_submit_trx (Path A) for WAC pools.
- Submit STD receipt via ledger_submit_trx_c with a standard_cost row present, verify trx_line at standard cost and variance posting_line for the actual-vs-standard delta.
- STD pool with no standard_cost row: verify RAISE EXCEPTION at hot-path time.
- Submit specific receipt + depletion via ledger_submit_trx_c, verify identical SQL footprint to Path A (specific bypasses provisional mode, §3.5).
- FIFO/LIFO pool with `provisional_basis = 'running_avg'`: depletion records aggregate unit_cost as the provisional.
- FIFO/LIFO pool with `provisional_basis = 'standard'`: depletion records standard_cost.unit_cost as the provisional.
- 'standard'-basis FIFO pool with no standard_cost row: verify RAISE EXCEPTION.
- Insufficient aggregate inventory under Path C: depletion exceeds aggregate qty → RAISE EXCEPTION → caller's tx aborts.
- Concurrent callers to hot FIFO pool via direct Path C: verify lock contention is minimal (lock-hold time per trx is constant w.r.t. pool's layer count). **This is one of two primary measurements Path C exists to validate.**

**ledger-routed-c (routed flavor) integration tests:**

- Submit FIFO depletion via ledger_enqueue_trx_c, poll for trx existence by (trx_type, source_id); verify trx + trx_line appear after committer processes.
- Concurrent submissions to overlapping FIFO pools: verify affinity grouping (one commit_group handles all overlap; single committer per group).
- Committer death mid-processing on a routed-c commit_group: verify orphan recovery picks up the commit_group; trx rows for submissions in the group either all exist (recovery committer's tx committed) or all don't (recovery committer reprocesses fresh). Same invariant as Path B.
- Failed submission within a routed-c commit_group: verify the failed submission is excluded, remaining submissions process successfully in the same tx WITHOUT pristine-replay (§14.9). The aggregate's qty/unit_cost end at the correct value given only the successful submissions.
- 'standard'-basis FIFO pool with 1000 concurrent submissions on one hot pool via ledger_enqueue_trx_c: verify the commit_group is processed in one PG tx with one pool_lock acquisition. **This is the second primary measurement Path C exists to validate — batching combined with provisional cost handling.**
- Postmaster crash with routed-c submissions in-flight: shmem lost, no trx rows from in-flight work, system accepts new submissions normally.
- Duplicate submission detection: same (trx_type, source_id) submitted twice via ledger_enqueue_trx_c. Pre-flight dedup (§5.4 step 5, reused) catches it; no UNIQUE escapes.

**Direct vs routed comparison tests:**

- Drive the same workload through ledger_submit_trx_c and ledger_enqueue_trx_c; verify final pool_state.aggregate qty matches (provisional unit_cost values may differ because batch processing changes within-batch running-average evolution; that's expected and not a bug — recalc/close deferred would converge both to the same authoritative cost).
- Identify the concurrency threshold where routed-c overtakes direct-c on throughput: at low concurrency direct wins (no router overhead); at high concurrency on hot pools routed wins (batched aggregate update).

Recalc/close tests deferred per §6.4.

### 9.5 Cross-path correctness

Path A and Path B should produce byte-identical results for the same workload (modulo timestamps, ids): final pool_state, trx_line content, posting_line content all match. This validates that Path B's batching doesn't change semantics.

Path C produces a different intermediate state by design:
- Path C trx_line content has provisional unit_costs (running-average values) for depletions on layer-tracked pools, with source_trx_line_id NULL.
- Path A/B trx_line content has authoritative FIFO/LIFO unit_costs with source_trx_line_id populated.
- Path C pool_state has only the aggregate row (layer_id=0) for layer-tracked pools; Path A/B have aggregate + layer rows.

These are intentional architectural divergences. Cross-path equivalence in the PoC is limited to: aggregate qty matches across all three paths (sum of receipts minus sum of depletions). Authoritative-cost equivalence between Path C and Path A/B is a recalc/close concern (deferred, §13) — once recalc/close runs against a Path C trx_line stream, the resulting trx_line history (provisional + adjustments) should sum to what Path A/B would have produced for the same workload.

### 9.6 Harness-driven measurement

The workload generator (`ledger-harness`) drives all three paths through identical workloads and records:

- Throughput: committed trxs/second.
- Caller latency: submit-to-acknowledge (Path A: caller's user-tx commit returns; Path B: enqueue function returns; Path C: caller's user-tx commit returns).
- Effective latency: submit-to-recorded (Path A: same as acknowledge; Path B: polling detects trx existence by (trx_type, source_id); Path C: same as acknowledge).
- Lock wait time per caller (Path A and C; Path C should be near zero on hot pools — primary Path C measurement).
- Eject count per submission (Path B).
- Submissions per commit_group (Path B).
- CPU burn per eject cycle (Path B): aggregate CPU time spent in router scan + committer pg_xact_status check per eject event. Detects when eject_cooldown is too short or in_progress callers are dominating worker time.
- Abort eviction rate (Path B): count of staging entries evicted because their caller's user_tx_xid is 'aborted' (per §5.4 step 4). High abort rates indicate callers frequently abort post-enqueue; if abort eviction CPU approaches committer's processing CPU, the queue is saturating with dead entries and admission control may be needed.
- Committer time breakdown (Path B): fraction of committer wall-clock spent on (a) pg_xact_status checks, (b) eviction CAS operations, (c) actual plan_apply work, (d) bulk SQL writes. If (a)+(b) dominate, the workload is in a degraded regime.
- Variance magnitude (Path C): for each depletion, the gap between provisional unit_cost and what a strict FIFO/LIFO computation would produce given the trx_line history at the time of the depletion. Computed offline by replaying the trx_line stream. Distribution of (true - provisional) / provisional. Tells us how wrong provisional costs are in steady state, which is informational data for whoever later implements recalc/close.
- fsync count per second (system-wide).
- WAL volume per trx.

## 10. Workload configurations

The harness simulates production conditions with varying parameters.

### 10.1 Caller concurrency

- 1 caller (baseline).
- 10 callers (light concurrent load).
- 50 callers (moderate).
- 200 callers (heavy).
- 1000 callers (stress).

Each caller is a separate PG session running a loop: submit trx, optionally poll until committed, repeat.

### 10.2 Pool universe and overlap

- Pool universe: 10,000 pools (e.g., 1000 SKUs × 10 locations).
- Overlap distribution:
  - Uniform: each caller picks a pool uniformly at random.
  - Zipfian (skewed): hot pools dominate; simulates real retail/manufacturing where a few SKUs are popular.
  - Disjoint: each caller has a private pool range (no overlap).

### 10.3 Trx complexity

- Simple: 1 line per trx (PO receipt of one item).
- Medium: 3-5 lines per trx (typical inv adjustment, partial WO).
- Complex: 10+ lines per trx (full WO completion with backflushes, transfers).

### 10.4 Method mix

- All WAC.
- All FIFO.
- Mixed: 60% WAC, 30% FIFO, 10% STD (representative of real deployments).

### 10.5 Pool depth (Path C-specific)

For workloads exercising layer-tracked methods, the existing layer depth on a pool affects strict-mode performance:

- Shallow: ≤10 live layers per pool. Path A/B's layer iteration is fast.
- Medium: 10-100 live layers per pool. Path A/B's lock-hold time per depletion grows linearly.
- Deep: 1000+ live layers per pool. Strict layer iteration dominates lock-hold time; this is where Path C should pull dramatically ahead.

Depth is established by pre-seeding the pool with receipts before the measurement workload begins.

### 10.6 Workload matrix

For the bake-off: cross-product of caller concurrency × overlap × complexity × method mix × pool depth. Not every combination needs full runs; pick representative scenarios:

- **S1**: Light concurrency, low overlap, simple trxs, all WAC, shallow pools. Baseline. All three paths should perform similarly (no contention, no layer iteration).
- **S2**: Heavy concurrency, high overlap, simple trxs, all WAC, shallow pools. Tests routing's lock-contention amortization. Path C is identical to Path A here (WAC has no layer iteration).
- **S3**: Light concurrency, low overlap, complex trxs, mixed methods, shallow pools. Tests per-trx work intensity.
- **S4**: Heavy concurrency, Zipfian overlap, complex trxs, mixed methods, shallow pools. Stress-test, production-like.
- **S5**: Pathological — 1000 callers, all hitting one hot pool, simple trxs, all WAC. Routing should win dramatically. Path C identical to Path A for WAC.
- **S6**: Pathological — 1000 callers, fully disjoint pools, simple trxs. Direct should win (no contention, lower overhead).
- **S7**: Heavy concurrency, Zipfian overlap, simple FIFO trxs, deep pools. **Path C's home field.** Path A/B should bottleneck on lock-hold time per depletion. Direct Path C reduces lock-hold time (each caller serializes through a microsecond-per-trx critical section). Routed Path C reduces the serialization itself (1000 concurrent submissions become one commit_group with one aggregate update). Both Path C flavors should pull ahead by 10x+; routed-c should overtake direct-c at high concurrency on overlapping pools.
- **S8**: Heavy concurrency, Zipfian overlap, complex FIFO trxs (multi-line), deep pools. Production-realistic FIFO stress. Same hot-path properties as S7 with more complex per-trx work — direct-c and routed-c should both demonstrate the constant-time-per-trx aggregate work; routed-c benefits more from batching the multi-line work.

(Settlement-saturation scenarios are deferred along with recalc/close, §13.)

## 11. Success criteria

### 11.1 Correctness

Path A and Path B produce identical pool_state and trx_line content for identical workloads. Path C produces a different trx_line stream (provisional costs and NULL source_trx_line_id on layer-tracked depletions) and a smaller pool_state footprint (aggregate only, no materialized layers). Cross-path equivalence in the PoC checks aggregate qty across all three paths (sum of receipts minus depletions). Authoritative-cost equivalence between Path C and Path A/B is a recalc/close concern, deferred per §13.

### 11.2 Direct path baseline

Path A under S1 (single caller, simple trxs) establishes the single-threaded baseline throughput against which other scenarios are compared. There is no a priori target; the measurement IS the baseline.

### 11.3 Routed path scaling

Path B under S2 (heavy concurrency, high overlap) should demonstrate effective batching — commit_groups averaging multiple trxs per group rather than commit_groups of size 1. If commit_group sizes stay at 1, the router isn't earning its complexity and Path B reduces to Path A with extra overhead.

### 11.4 Path C demonstration

Two demonstrations:

**Direct Path C under S7 (heavy concurrency, FIFO, deep pools)** should show dramatically lower lock-hold time per trx than Paths A and B. If lock-hold time scales with pool depth (rather than being constant), the architectural premise has failed.

**Routed Path C under S7-S8 (heavy concurrency, FIFO, deep pools)** should show throughput that exceeds direct Path C on hot pools — the batched commit_group reduces 1000 individual pool_lock acquisitions to one. This is the answer to "what happens when 1000 callers hit one deep FIFO pool simultaneously": neither Path A (lock-hold = layer iteration), nor Path B (lock-hold = layer iteration inside committer), nor direct Path C (1000 serialized microsecond turns) gives a satisfying answer at very high concurrency. Routed Path C does.

Recalc/close throughput, settlement lag, convergence latency, and steady-state variance distribution are all deferred concerns (§13); they get measured when recalc/close is built.

### 11.5 Crossover identification

Identify the concurrency × overlap × method × depth region where each path wins. Four paths to compare:

- **Path A (direct)** wins: low concurrency, disjoint pools, any method, any depth. No router overhead, no shmem hop.
- **Path B (routed, strict)** wins: high concurrency, overlapping pools, any method, **shallow pools**. Batching amortizes pool_lock acquisition across submissions; strict layer math is cheap when there are few layers to iterate.
- **Direct Path C** wins: moderate concurrency, layer-tracked methods, deep pools. Lock-hold reduction matters; batching overhead doesn't pay off at moderate concurrency.
- **Routed Path C** wins: high concurrency, layer-tracked methods, deep pools. Lock-hold reduction + batching combine to handle the regime where every other path bottlenecks.

The Path B (strict) vs Path A and routed-c vs direct-c crossovers should be at roughly the same concurrency thresholds for shape reasons — routing makes sense when you have enough concurrent overlapping submissions to make commit_groups non-trivial in size.

The Path C value proposition depends on a working recalc/close mechanism to convert provisional costs to authoritative ones; the PoC validates the hot path (the lock-hold and batching wins) but not the full Path C value proposition. Mid-period cost accuracy under Path C is provisional until recalc/close.

The crossover surfaces are what the PoC characterizes empirically. Hypothesis going in: routed Path C dominates the deep-FIFO regime so dramatically that the other three paths are not viable for production FIFO workloads at scale — matching what every major ERP has independently concluded. The PoC validates or falsifies this.

### 11.6 Failure mode coverage

**Path A** failure scenarios: caller's user-tx aborts on any error (RAISE EXCEPTION). No trx row created. No partial state. Caller sees the SQL exception.

**Path B** failure scenarios, with the invariant that **trx exists iff successfully recorded**:

- **Caller user-tx aborts before committer processes** (Path B variant that carries caller's user_tx_xid in shmem): committer detects the abort, drops the submission from the batch. No trx row is created. Caller knows their tx didn't commit; nothing to look up.
- **Caller user-tx held open past caller_tx_timeout**: committer ejects with cooldown. After timeout, the submission is dropped. No trx row is created.
- **Committer dies mid-processing**: orphan recovery picks up the commit_group. If the dead committer's PG tx committed (some trx rows already exist), the recovery committer sees them via UNIQUE constraint conflicts and excludes those submissions from its replay. If the dead committer's PG tx aborted, no trx rows exist and the replay creates them fresh. Either way: each submission ends with exactly zero or one trx row.
- **Router dies mid-routing**: shmem entries get reverted to pending by the boot sweep. Submissions are re-routed. No DB effect.
- **Postmaster crash**: shmem lost. In-flight submissions lost. No DB orphans (no trx rows were written for in-flight work). Previously-committed trx rows are durable and intact.

**Path C** failure scenarios:

- **Hot path failure** (e.g., insufficient aggregate inventory): same as Path A — RAISE EXCEPTION, caller's tx aborts, no trx row created.
- **Postmaster crash**: in-flight hot-path trxs (uncommitted) aborted by PG; already-committed trxs durable.

Recalc/close failure modes are deferred (§13).

In all cases: a trx row in the database represents a successfully recorded business event with a provisional cost. Its absence represents either "never submitted" or "submitted but didn't complete." Authoritative cost is established later by recalc/close.

## 12. PoC implementation plan

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

### Phase 6a: ledger-direct-c (Path C direct flavor)

- pgrx extension exposing ledger_submit_trx_c.
- Integrates ledger-core (uses plan_apply_provisional dispatch for FIFO/LIFO; plan_apply for WAC/STD/specific).
- Bulk write logic for aggregate-only updates (FIFO/LIFO under provisional mode) and full layer mutations (specific under strict mode).
- standard_cost lookup logic for STD pools and 'standard'-basis FIFO/LIFO pools.
- Pool_lock acquisition (brief, aggregate-row work only for provisional mode).
- Error handling: caller-visible failure mode (same as Path A).

Deliverable: Path C direct flavor operational. Submitting a FIFO depletion via ledger_submit_trx_c writes a trx_line with provisional unit_cost, updates only the aggregate row of pool_state, and returns. No layer iteration.

### Phase 6b: ledger-routed-c (Path C routed flavor)

- pgrx extension exposing ledger_enqueue_trx_c.
- Reuses Path B's shmem layout, router worker, and committer worker pool (Phase 4 deliverables).
- Committer inner work: plan_apply_provisional dispatch instead of plan_apply.
- **No pristine-snapshot replay** (per §6.3 and §14.9 — Path C has no cross-trx state dependency).
- Failed-submission handling: drop from working snapshot and continue, rather than restart.

Deliverable: Path C routed flavor operational. 1000 concurrent ledger_enqueue_trx_c submissions to one hot FIFO pool become one commit_group processed in one PG tx with one pool_lock acquisition and one aggregate UPDATE.

### Phase 7: ledger-harness, Path C measurements

- Add direct-c and routed-c drivers to harness.
- Drives both Path C flavors through scenarios S1-S8.
- Records hot-path throughput, lock-hold time, variance magnitude (computed offline by replaying trx_line stream).
- Direct vs routed crossover measurement: find the concurrency threshold where routed-c overtakes direct-c.

Deliverable: Path C throughput numbers (both flavors) across the workload matrix.

### Phase 8: comparison and characterization

- Run all four path variants (A, B, direct-c, routed-c) against identical workloads.
- Build crossover map: concurrency × overlap × method × depth → which path wins on hot-path throughput.
- Document findings.
- Identify regimes where one path dominates or where the tradeoffs are subtle.
- Validate (or falsify) the hypothesis that routed Path C dominates the deep-FIFO + high-concurrency regime.

Deliverable: PoC report with empirical data on the four-way hot-path tradeoff.

Recalc/close implementation is a separate future phase, outside this PoC's scope.

## 13. What's out of scope for this PoC

- Multi-currency. Single currency assumed. amount in posting_line is in one implied currency.
- account_balance denormalization. Balances derived via SUM(amount) at query time. Production deployments would maintain a denormalized table.
- Hot-path FIFO audit queries (full layer-history reconstruction). Direct query of trx + pool_state suffices; materialized views can be added later.
- Period close mechanics. accounting_period table exists but the close hook (drain-to-zero pattern) is not implemented in the PoC. v2.1's §10 mechanics carry over when needed.
- Webhook delivery. v2.1's webhook_deliveries machinery is omitted; the PoC measures throughput, not external notification.
- Multi-tenant isolation. Single-tenant.
- Identity-key extended dimensions beyond what pool.identity_key carries. Lot-tracking and unit-tracking work; arbitrary user-defined dimensions on inventory (project, customer, etc.) are deferred — they could be added as additional columns on pool or via a separate identity-attribute table.
- **Recalc / close (authoritative FIFO/LIFO cost reconciliation for Path C).** Path C records provisional costs on the hot path; turning them into authoritative FIFO/LIFO costs by walking the trx_line stream, running layer math, and posting cost-adjustment trxs is deferred. This covers: the worker/process model (continuous, on-demand, periodic batch); the schema additions (cost_adjustment enum values, sequences, watermarks or settled-state on trx_line, layer-row materialization); concurrency between hot path and reconciliation; recovery semantics; backdated-receipt handling; audit linkage between depletions and the receipt layers that fed them. The PoC measures the hot path; reconciliation is built later based on what the production workload demands. See §6.4.

## 14. Concerns and open questions

### 14.1 trx_line ordering and id allocation

trx_line uses BIGSERIAL for its primary key. Ordering within a pool uses `trx_line.id` ascending (BIGSERIAL is globally monotonic in allocation order).

The earlier v3 design used a per-pool `trx_seq` column with `UNIQUE (pool_id, trx_seq)`. That approach was dropped because it required out-of-band sequence reservation under Path C and created unworkable lock-coordination problems. trx_line.id alone — globally monotonic, allocated by BIGSERIAL with no coordination — is sufficient for within-pool ordering on the hot path.

Concerns:

- **id is globally monotonic, not per-pool dense.** A pool's trx_line ids will be sparse (e.g., 5, 17, 42, 109) because other pools take intervening values. Doesn't matter for ordering; only the relative order within a pool matters.
- **BIGINT exhaustion.** BIGSERIAL is signed 64-bit; max value ~9.2 × 10^18. Not a concern.
- **Backdated receipts.** A receipt with business-effective posted_at preceding earlier-allocated receipts in the same pool gets a HIGHER trx_line.id (because BIGSERIAL is allocation-order, not posted_at-order). Hot-path FIFO uses layer_id (= receipt's trx_line.id) ascending; the backdated receipt appears at the end of the layer stack, not at its business-effective position. This is acceptable for the PoC's hot-path scope (matches Dynamics-style FINANCIAL-date behavior). Recalc/close (deferred) would correct this if business-effective ordering is required — by joining trx for posted_at, by denormalizing posted_at onto trx_line, or by using a Cost Date derivation. That's a recalc/close design decision.

### 14.2 Pool registration

Two options:

(a) Explicit: an operator (or seed function) INSERTs into pool before any trx references it. trx submission errors if the pool doesn't exist.

(b) Lazy: ledger_submit_trx / ledger_enqueue_trx auto-creates pool on first reference using (sku_id, location_id, identity_key, method) provided by the caller.

Lazy is easier for development; explicit is safer for production (forces deliberate configuration). Recommend explicit for the PoC: it surfaces missing-pool errors as deliberate failures rather than silent auto-creation.

### 14.3 Stale pool_state rows after total depletion

Under WAC, when a pool's qty drops to zero, the pool_state row stays (qty=0, unit_cost=preserved). When the pool is later replenished, the next receipt establishes a fresh basis. Operational concern: pool_state grows with one row per pool ever touched, even if currently empty. For most workloads this is fine (pool count is bounded by sku × location). Could be cleaned by a periodic vacuum process. Not a hot-path concern.

### 14.4 commit_group has no DB representation

The commit_group concept exists only in shmem. It groups multiple submissions for processing in one committer PG tx. After commit, the trx rows from a single commit_group all share the same `created_at` timestamp (within microseconds) and were written together, but there is no explicit commit_group_id column on trx. "What was committed together" is not a question the schema is built to answer — the queue layer can record this if needed (and it's lost on postmaster crash regardless), but the trx layer doesn't carry it.

If future audit needs require grouping (e.g., "show me all trxs that landed in the same committer batch"), a commit_group table can be added then.

### 14.5 Direct-path failure handling

When ledger_submit_trx encounters InsufficientInventory, UNIQUE constraint violation, or any other failure, it RAISEs EXCEPTION. PG aborts the caller's user-tx. No partial writes survive — the trx row, trx_line rows, pool_state mutations, and posting_line rows all roll back together because they were in the same tx. The caller sees the SQL exception.

This is the only failure mode for Path A. The caller's PG client decides what to do with the exception (retry, log, abort their own work).

### 14.6 Source_id type

source_id on trx and trx_line is BIGINT. Limits source tables to BIGINT primary keys. acct uses BIGINT throughout, so this works for the PoC. Future external integrations might require TEXT or UUID source_ids; defer until needed.

### 14.7 Account discovery for posting_lines

The ledger needs to know which accounts to debit/credit for each trx_type / line_type. Two approaches:

(a) Caller provides debit_account and credit_account in the SPI call. Simple, flexible, places the burden on the caller.

(b) Ledger has a rules table mapping (trx_type, line_type) → (debit_account, credit_account) for each (sku, location, dimensions). Caller doesn't specify; ledger looks up.

(a) is right for the PoC. (b) is a real production concern but separate from the cost-ledger PoC scope.

### 14.8 Path C: aggregate qty and unit_cost for FIFO/LIFO pools

The pool_state aggregate (layer_id = 0) for a FIFO/LIFO pool under Path C carries qty (the running net) and unit_cost (the running average maintained per WAC formula on every receipt). The aggregate qty is the authoritative on-hand count; this matches what Path A/B would produce (sum of layer qtys = aggregate qty). The aggregate unit_cost is NOT what Path A/B would produce — for layer-tracked methods, Path A/B don't maintain a running average at all. The aggregate unit_cost under Path C is a Path C-specific construct.

For pools with `provisional_basis = 'running_avg'`, this aggregate unit_cost IS the provisional cost basis used on depletions. For pools with `provisional_basis = 'standard'`, the aggregate unit_cost is still maintained (the WAC formula still runs on receipts) but is not used for depletions — depletions pull from standard_cost instead. The aggregate's running average remains useful for analytical queries even when not on the hot read path.

If a query reads `pool_state` for a FIFO pool and finds layer_id=0 with unit_cost=X, that X is the running average, not the "current FIFO cost" (which doesn't have a single meaningful value for a multi-layer pool). Queries needing layer-specific costs must wait for recalc/close (deferred) or replay trx_line directly.

Specific pools have aggregate semantics identical to Path A (they use strict mode even on Path C, §3.5) — the aggregate just tracks the sum of all live single-unit layers.

### 14.9 Path C: pristine-replay is not used

Path B uses pristine-snapshot replay (§8) to handle failures in a commit_group where one trx's plan_apply fails and excluded trxs would have left stale intermediate state visible to other trxs. Path C has no cross-trx state dependency on the hot path — each trx updates only the aggregate row and produces only its own trx_line/posting_line rows. A trx that fails plan_apply_provisional (e.g., depletion exceeds aggregate qty) is simply dropped from the working snapshot; remaining trxs proceed unchanged.

For direct flavor: the failed trx's user-tx aborts via RAISE EXCEPTION; nothing else is at stake.

For routed flavor (§6.3): the failed submission is excluded from the commit_group's working snapshot. Remaining submissions continue using the snapshot. The aggregate's qty/unit_cost updates are commutative (receipts and depletions are additive; the WAC running-average formula is order-sensitive on receipts but produces a deterministic result given the SET of receipts in the batch, regardless of within-batch processing order — and any cross-batch ordering "errors" are corrected by recalc/close).

This is a meaningful simplification of routed Path C versus Path B routed: no pristine-replay machinery, no restart-from-snapshot logic, no double-pass over the commit_group on failure. Failed submissions are dropped, work continues.

### 14.10 Path C: choice of provisional cost basis

For FIFO/LIFO pools under Path C, the hot path needs to record SOME unit_cost for depletions, even though the true FIFO/LIFO cost is unknown at recording time. The PoC supports two bases, selectable per pool via `pool.provisional_basis` (§2.2):

**`'running_avg'` (default).** Use the pool aggregate's running unit_cost, maintained per the WAC formula on every receipt. Self-contained — no external table lookup. Tracks recent receipts; generally close to true cost for slow-moving pools; can deviate when receipt costs are volatile. The default choice for pools where no operator-maintained standard exists.

**`'standard'`.** Use the standard_cost table's unit_cost for the pool's (sku_id, location_id). Predictable; the variance recalc/close has to correct is bounded by deliberate standard-vs-actual deltas, not by recency-of-receipt accidents. Requires standard_cost to be populated for the sku/location — fail-loud if missing (RAISE EXCEPTION at hot-path time). Best for pools where the business already maintains standards.

Two additional bases were considered and not implemented:

- **Last receipt's unit_cost.** Closer to LIFO-true for stable-cost pools, but stale for FIFO and doesn't smooth volatility. Requires reading the most recent receipt trx_line. Not in scope; could be added as a third `pool_provisional_basis` enum value.
- **Last applied depletion cost.** Strong recency bias on volatile pools. Not in scope.

Both implemented options produce identical lock-hold characteristics on the hot path: `running_avg` reads one column from pool_state, `standard` reads one column from standard_cost. Either way it's a constant-time read followed by an aggregate update. The basis choice affects only variance magnitude that recalc/close (deferred) would later correct, not what the PoC measures.

**Basis × routed flavor interaction.** Under routed Path C (§6.3), the basis choice has a subtle batching consequence:

- `'running_avg'`: the committer's working snapshot maintains the running average across batch members. Receipts within the batch update old_unit_cost; subsequent depletions in the same batch see the updated value. The recorded provisional cost for a depletion thus depends on the batch's internal processing order — different from what direct flavor would record if the same submissions arrived individually. This is not wrong, just different — and the difference washes out at recalc/close.
- `'standard'`: the committer reads standard_cost once at hydration; every depletion in the batch uses the same value. No within-batch state evolution. Pure data shuffle. Simplest, fastest path through the committer.

For pools where the business already maintains standard costs, `'standard'`-basis routed Path C is the architecturally cleanest configuration — no order-dependency within or across batches, fully order-independent recording.

The basis can be changed by operators (UPDATE pool SET provisional_basis = ...) at any time. The change applies to future depletions; already-recorded trx_lines retain their provisional costs as recorded. Recalc/close would correct any inconsistency at reconciliation time.

### 14.11 Path C and reverse operations

A reverse/cancel of a previously-recorded trx is a new business event: it would be recorded as a new trx with negative qty signaling the reversal. The hot path treats it like any other depletion or receipt, updating the aggregate per the §3.1 WAC formula.

Layer-level "marking" (Dynamics 365's feature where a reversal exactly releases the layers consumed by the original) requires recalc/close to interpret the source_trx_line_id linkage. The schema already supports the linkage (source_trx_line_id column on trx_line); the recalc/close logic to honor it is deferred.
