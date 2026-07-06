# design-v3.1: PoC for cost-ledger architecture — Path C

## 1. Purpose

Specify Path C of the cost-ledger architecture: the provisional hot path with deferred recalc/close.

This document is self-contained — it carries the full schema, method semantics, ledger-core layout, routing infrastructure, and testing strategy needed to implement Path C end-to-end. It does not depend on any other design document for implementation details. The companion design-v3.md specifies Paths A and B (direct and routed strict-mode) using the same schema and ledger-core; Path C drops into that architecture as a third path, but an implementer reading v3.1 alone has everything needed to build Path C.

**What Path C is.** Hot path (either direct-style or routed-style) records each trx with a provisional unit_cost computed from the pool's running aggregate or a standing standard cost. For FIFO/LIFO pools, layer-tracked state is not touched on the hot path — only the aggregate row of pool_state is updated. The trx_line stream is the durable record of what arrived and at what provisional cost. Turning those provisional costs into authoritative FIFO/LIFO costs — by walking the trx_line stream, running strict layer math, and posting cost-adjustment trxs for the variance — is a recalc/close concern. **Recalc/close is out of scope for this PoC** (§13). The PoC measures the hot-path divergence only.

For WAC and STD pools, there is nothing to reconcile; Path C's hot path produces the final cost directly and behaves identically to a strict-mode implementation. The architectural divergence applies specifically to FIFO and LIFO. Specific pools have K=1 layer count and bypass provisional mode entirely (§3.4).

This pattern is what every major production ERP does. SAP S/4HANA operationally values inventory at Standard Price (Price Control S) and runs Actual Costing (CKMLCP) at period-end to revalue consumption. Oracle Fusion Cost Management runs a continuous background Cost Processor. Dynamics 365 F&O records issues at a Running Average Cost Price and runs Inventory Close at period-end. All three vendors independently arrived at the conclusion that strict in-order FIFO/LIFO on the hot path does not scale.

**The PoC measures**:
- Hot-path throughput and lock-hold time under FIFO/LIFO at deep-pool, high-concurrency workloads where strict-mode paths bottleneck.
- The direct flavor vs routed flavor crossover for Path C — where batching pays off relative to per-caller-tx semantics.

**The PoC defers**:
- Recalc/close (the mechanism that converts provisional costs to authoritative ones).

## 2. Schema

Greenfield. No migration from existing systems.

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

`cost_adjustment` is NOT a trx_type, line_type, or posting_event_type in this PoC. It would be added by recalc/close (out of scope, §13) when implemented. ALTER TYPE ADD VALUE is a trivial migration.

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
    UNIQUE (sku_id, location_id, identity_key),
    CHECK (method != 'specific' OR identity_key != 0)
);

CREATE TABLE standard_cost (
    sku_id       BIGINT NOT NULL,
    location_id  BIGINT NOT NULL,
    unit_cost    BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sku_id, location_id)
);
```

`pool.provisional_basis` controls what the Path C hot path uses as the provisional unit_cost on depletions for FIFO/LIFO pools (see §5 and §14.3):
- `'running_avg'` (default): use the pool's aggregate row's unit_cost, maintained per the WAC formula on every receipt. Self-contained — no external table lookup needed.
- `'standard'`: use the standard_cost table's unit_cost for (sku_id, location_id). Requires standard_cost to be populated for the pool's sku/location.

Only meaningful for FIFO/LIFO pools. Specific pools bypass Path C provisional mode entirely (§3.4) — they run strict layer math even on Path C, so the column is ignored. WAC and STD pools have no provisional/strict distinction — `provisional_basis` is ignored for these too.

`standard_cost` is keyed by (sku_id, location_id) — one current standard cost per sku/location pair. No effective-dating in this PoC; rows are updated in place when standards revise. (Production deployments needing temporal standards add effective_from/effective_to columns; backdated trxs would then look up the standard active on their posted_at.) Used by:
- STD pools (`pool.method = 'std'`): receipts/depletions both record at standard_cost.unit_cost; variance between actual and standard posts to a variance account on receipts (§3.3).
- FIFO/LIFO pools with `provisional_basis = 'standard'`: hot path uses standard_cost.unit_cost as the provisional cost for depletions, instead of the pool aggregate's running average.

If a STD pool or a 'standard'-basis pool is referenced and no standard_cost row exists for its (sku, location), RAISE EXCEPTION at hot-path time. Configuration error, fail loud.

pool.method is treated as immutable after pool creation. Changing it produces corrupted pool_state. Production deployments enforce via trigger or revoked UPDATE grant; PoC does not enforce.

pool carries no settlement watermarks. Recalc/close (deferred, §7) would add what it needs when implemented — most likely a watermark or settled-state mechanism on trx_line. None of that is in PoC scope.

```sql

CREATE TABLE pool_state (
    pool_id     BIGINT NOT NULL REFERENCES pool(id),
    layer_id    BIGINT NOT NULL,
    qty         BIGINT NOT NULL,
    unit_cost   BIGINT NOT NULL,
    value_sum   BIGINT NOT NULL,           -- cumulative book value; unit_cost is DERIVED as banker_div(value_sum, qty) (§3.0/§3.1)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pool_id, layer_id)
);
```

`layer_id` identifies the row's role within the pool:
- `layer_id = 0` is the **aggregate row**. Every pool has exactly one. Carries running qty and unit_cost (running average for WAC and for Path C provisional basis).
- `layer_id > 0` is a **materialized layer row** (only used by specific pools under Path C, since FIFO/LIFO under Path C does not materialize layers on the hot path). The layer_id IS the receipt trx_line's id — i.e., `pool_state.layer_id = trx_line.id` for the receipt trx_line that created the layer.

For WAC pools, only the aggregate row exists. For STD pools, the aggregate row is required for qty tracking (so the no-negative-inventory invariant per §3.6 can be enforced); see §3.3. For specific pools, both aggregate and layer rows exist. For FIFO/LIFO pools under Path C, only the aggregate exists — layer rows would be materialized by recalc/close, deferred.

```sql

CREATE TABLE pool_lock (
    pool_id     BIGINT PRIMARY KEY REFERENCES pool(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE trx (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trx_type     trx_type NOT NULL,
    source_id    BIGINT NOT NULL,
    posted_at    TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (trx_type, source_id)
);

CREATE TABLE trx_line (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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

trx_line.id is an auto-allocated identity column (`GENERATED ALWAYS AS IDENTITY`, which uses an implicit sequence under the hood and is functionally equivalent to BIGSERIAL). It is globally monotonic in allocation order. Ordering within a pool uses trx_line.id directly. No per-pool sequence column; trx_line.id is allocated by PG with no extra coordination.

trx.source_id is the application-managed identifier of the source business document — e.g., for a PO receipt the source_id is the upstream PO system's receipt document ID. Combined with trx_type, it forms the UNIQUE constraint that serves as the ledger's idempotency key: re-submitting the same (trx_type, source_id) pair raises a constraint violation rather than creating a duplicate trx. This is what lets Path B/C routed flavors recover from committer death without writing duplicate trx rows — the recovering committer either sees the trx already exists (skip) or doesn't (re-process).

trx_line.source_id is an application-managed identifier referencing the source business document's line — e.g., for a PO receipt with three lines, the trx_line.source_id values would be the upstream receiving system's line IDs (1, 2, 3). The ledger does not interpret or constrain this value; it exists to let downstream queries trace ledger trx_lines back to their originating business document line. The PoC harness can use any value (including 0 or sequential test IDs); production deployments populate it from their source systems. NULL is allowed for trx_lines that have no meaningful source line (e.g., system-generated reconciliations).

trx_line.source_trx_line_id is a different column — it is a self-reference within the ledger, populated only for depletion trx_lines that consume a specific receipt's layer (FIFO/LIFO strict mode under Paths A/B, and specific-id mode under all paths). Path C's hot path leaves it NULL for FIFO/LIFO depletions because provisional mode doesn't commit to a specific source layer. Recalc/close (deferred) would populate it during authoritative cost reconciliation.

trx.posted_at is the business-effective time provided by the caller. trx_line does not denormalize posted_at — recalc/close (deferred) would JOIN trx if business-effective-time ordering is required.

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

CREATE INDEX account_parent ON account (parent_id) WHERE parent_id IS NOT NULL;

CREATE TABLE posting_line (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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

For PoC scope, sku and location carry the minimum needed for the ledger to reference them. Production schemas would extend with all the domain attributes.

**ID allocation across tables.** trx, trx_line, and posting_line use `GENERATED ALWAYS AS IDENTITY` — IDs are auto-allocated by PG during INSERT; the ledger never specifies them. pool, account, accounting_period, sku, and location use `BIGINT PRIMARY KEY` without auto-allocation — these IDs are application-managed (assigned by whichever upstream system owns the reference data, e.g., an inventory system assigning sku IDs and a finance system assigning account IDs). The PoC harness assigns these IDs explicitly during test data setup. If a deployment prefers auto-allocation for any of these reference tables, switching to `GENERATED ALWAYS AS IDENTITY` is a one-line schema change.

## 3. Method semantics

Each pool_method determines how trx_line rows interact with pool_state. Path C runs strict mode for WAC, STD, and specific methods; it runs **provisional mode** for FIFO and LIFO (§3.5).

### 3.0 Numeric representation and rounding

**Storage.** All monetary values (`unit_cost`, posting_line.amount, etc.) and quantities (`qty`) are stored as `BIGINT` with implicit fixed-point precision. The PoC uses **1 BIGINT unit = 1 micro-currency-unit (1e-6)** — i.e., one dollar is 1,000,000; one cent is 10,000; one mill (1/1000 of a cent) is 10; the smallest representable amount is 1 micro-cent (1e-6 of a currency unit). BIGINT range is ~9.2 × 10^18, so the representable range at this precision is ~±9.2 × 10^12 currency units (more than nine trillion dollars).

Production deployments wanting exact decimal arithmetic at arbitrary precision should swap BIGINT for PG's `NUMERIC(precision, scale)` type. Schema-change-and-recompile; the ledger-core arithmetic logic stays the same shape.

**Aggregate book value (`value_sum`).** Each `pool_state` row carries a `value_sum` BIGINT alongside `qty` and `unit_cost`. For the aggregate row (`layer_id = 0`) it is the cumulative book value of the pool, and `unit_cost` is **derived** from it as `banker_div(value_sum, qty)` — a rounded view, not an independent accumulator. Receipts add `Q × C` exactly; depletions subtract the posted depletion amount (`Q × applied_unit_cost`), so `value_sum` stays equal to the net of the pool's posting_line amounts (GL-reconcilable). For layer rows (`layer_id > 0`, specific) `value_sum = qty × unit_cost`. Storing the exact book value — rather than re-deriving the average incrementally off the *previously-rounded* average — is what makes a receipts-only pool's final `unit_cost` exactly `banker_div(Σ Q×C, Σ Q)` and independent of receipt order. (Mixed receipt+depletion sequences stay order-sensitive: depletion-at-rounded-average is lossy. §3.1, §11.1, §14.2.)

**Division and rounding.** Deriving the running-average `unit_cost` from `value_sum` produces a non-integer ratio that must be coerced back to BIGINT. The division site:

- Running-average derivation (§3.1): `unit_cost = value_sum / qty`, where `value_sum` is the exact accumulated book value. Used both by strict WAC mode and by Path C's provisional mode (which maintains the aggregate book value on the aggregate row for FIFO/LIFO pools).

ledger-core uses **banker's rounding** (round-half-to-even) for this division. Rationale: under sustained workloads with many small fractional remainders, biased rounding (always-truncate, always-round-up) accumulates a systematic drift in the pool's recorded value. Banker's rounding is symmetric — half-way cases round to the nearest even integer, which over a sufficiently random distribution of remainders cancels out to net-zero drift.

The Rust helper:

```rust
/// Banker's rounding (round-half-to-even) for integer division.
/// Takes an i128 numerator (callers must cast i64 operands to i128 BEFORE
/// multiplying — i64 * i64 can overflow at the precision levels this ledger
/// uses) and an i64 denominator. Returns the quotient as i64, panicking on
/// overflow if the true quotient cannot fit in i64 (which would indicate a
/// real numeric error at the application level — e.g., a pool's value
/// exceeding the representable range).
///
/// Exact-half cases round to the nearest even integer.
/// Panics if denominator == 0. Caller must guard.
pub fn banker_div(numerator: i128, denominator: i64) -> i64 {
    debug_assert!(denominator != 0, "banker_div: denominator must be non-zero");
    let denom = denominator as i128;
    let q = numerator / denom;
    let r = numerator % denom;

    let rounded: i128 = if r == 0 {
        q
    } else {
        let abs_r = r.abs();
        let abs_d = denom.abs();
        let sign: i128 = if (numerator < 0) ^ (denominator < 0) { -1 } else { 1 };
        let twice_r = abs_r * 2;
        if twice_r < abs_d {
            q
        } else if twice_r > abs_d {
            q + sign
        } else {
            // Exactly half: round to nearest even.
            if q % 2 == 0 { q } else { q + sign }
        }
    };

    // Final downcast to i64; panic if the result doesn't fit (application-
    // level overflow — pool value exceeds representable range).
    i64::try_from(rounded).expect("banker_div: result overflows i64; pool value out of range")
}
```

**Why i128 numerator?** Two places need i128. (1) Accumulating `value_sum += Q × C`: each `Q × C` is up to `i64 × i64`, which overflows i64 well before the values are unreasonable, so the product is formed in i128 before adding. (2) The derivation `banker_div(value_sum, qty)` takes `value_sum` as an i128 numerator. At the PoC's 1e-6 precision, a pool with qty = 10^8 units and unit_cost = $10,000 (= 10^10 BIGINT-units) already has `value_sum ≈ 10^18`, near i64's limit (~9.2 × 10^18). Callers must promote to i128 before multiplying:

```rust
// Correct:
let new_value_sum: i128 = (old_value_sum as i128) + (q as i128) * (c as i128);
let new_unit_cost = banker_div(new_value_sum, new_qty);

// Wrong (silent overflow at i64 level before the i128 widening):
let bad: i64 = old_value_sum + q * c;  // q*c overflows
```

The function signature enforces this discipline — passing an i64 expression to banker_div requires an explicit `as i128` cast, which is exactly where the caller should be doing the multiplication anyway.

**Final downcast.** banker_div's return is i64 because pool_state.unit_cost is BIGINT. If the true rounded quotient doesn't fit in i64, the pool's running-average unit_cost has exceeded representable range — a real numeric error that the application must handle (likely by raising an exception and refusing the trx). The `try_from` panic surfaces this cleanly; production deployments may wrap banker_div in a Result-returning shim to convert the panic into a graceful error.

**Bounded per-trx loss.** Even with banker's rounding, individual divisions still produce a residual of up to 0.5 LSB (half a micro-cent). That residual is lost — the ledger doesn't carry forward fractional precision below the LSB. For most workloads this is invisible. Cumulative worst-case across 10^9 ops is ~0.5 currency units — trivial relative to typical ledger balances. More rigorous treatment (residual columns, penny-rounding variance postings) is a recalc/close design concern.

### 3.1 WAC

pool_state has exactly one row per pool, at layer_id = 0 (the aggregate row). It carries the pool's total qty, the cumulative book value value_sum, and the derived running-average unit_cost (= `banker_div(value_sum, qty)`).

**Receipt** of qty Q at unit_cost C:
- INSERT trx_line (qty=Q, unit_cost=C). trx_line.id auto-assigned.
- UPSERT pool_state at layer_id=0 (old_qty = old_value_sum = 0 if the row does not exist yet):
  - new_qty = old_qty + Q.
  - new_value_sum = old_value_sum + (Q as i128) × (C as i128), stored as BIGINT. **Exact — no rounding.** The i128 product is mandatory (§3.0).
  - new_unit_cost = banker_div(new_value_sum, new_qty) if new_qty > 0; else preserve old_unit_cost (defensive guard against a degenerate qty=0 receipt against an empty pool, which would divide by zero). Banker's rounding per §3.0.
- The new_qty == 0 guard is defensive but load-bearing — without it, a degenerate receipt could panic ledger-core. Production callers should reject zero-qty receipts at the SPI layer; the guard exists as defense-in-depth.

Because value_sum accumulates exactly, a receipts-only pool's final unit_cost is `banker_div(Σ Q×C, Σ Q)` regardless of the order the receipts arrived — see §14.2. Under the PoC's no-negative-inventory invariant (depletions raise InsufficientInventory rather than driving qty below zero, see below), old_qty is always ≥ 0 going into a receipt. For Q > 0, new_qty > 0, so the formula branch is taken. Negative-qty handling is a deferred extension; see §3.6.

**Depletion** of qty Q:
- Read pool_state at layer_id=0. If qty < Q, RAISE EXCEPTION InsufficientInventory. The PoC does not allow depletions that would drive aggregate qty below zero.
- applied_unit_cost = current pool_state.unit_cost (the running average).
- INSERT trx_line (qty=-Q, unit_cost=applied_unit_cost).
- UPDATE pool_state at layer_id=0: new_qty = qty - Q. **value_sum -= Q × applied_unit_cost** (the posted depletion amount), so value_sum stays equal to the net of posted amounts; if new_qty == 0, value_sum := 0 to clear the rounding residual. new_unit_cost = banker_div(value_sum, new_qty) if new_qty > 0, else preserve. The average is preserved up to rounding — it is the exact book value, divided by remaining qty, that is authoritative, not a byte-stable average held across the depletion.

Path C's hot path runs this exact strict logic for WAC pools.

### 3.2 FIFO and LIFO

Strict FIFO/LIFO semantics (where depletions consume layer-by-layer in receipt order) apply to Paths A and B. **Under Path C, FIFO and LIFO pools use provisional mode (§3.5).** The hot path does not run strict layer math; only the aggregate row is mutated, and per-layer state remains unmaterialized until recalc/close.

For reference: a strict FIFO depletion of qty Q consumes layers from oldest first, INSERTs one trx_line per layer-portion touched (with the layer's unit_cost and source_trx_line_id pointing at the receipt's trx_line.id), and UPDATEs/DELETEs layer rows accordingly. LIFO reverses the ordering. None of this happens on Path C's hot path.

### 3.3 STD

No layer rows. Standard costs live in the `standard_cost` table (§2.2), keyed by (sku_id, location_id).

**Receipt** of qty Q at actual unit_cost C_actual:
- Look up standard_cost.unit_cost = C_std. If no row, RAISE EXCEPTION.
- INSERT trx_line with unit_cost = C_std.
- INSERT posting_lines:
  - Inventory account: debit Q × C_std.
  - Source account (AP, WIP, etc.): credit Q × C_actual.
  - Variance account: debit/credit the difference Q × (C_actual - C_std) — purchase price variance.

**Depletion** of qty Q:
- Look up standard_cost.unit_cost = C_std.
- INSERT trx_line with unit_cost = C_std.
- INSERT posting_lines per the depletion's debit/credit accounts at Q × C_std.

The inventory / source / variance accounts named here are resolved from `posting_account_map`, not supplied per line (v3.2 — see §3.7). The variance leg uses that table's `variance_acct`.

STD pools MUST maintain an aggregate row at layer_id = 0 (same structure as WAC: qty + unit_cost), so the no-negative-inventory invariant (§3.6) can be enforced uniformly. On every receipt: UPSERT pool_state (qty += Q; unit_cost = C_std mirrored from standard_cost). On every depletion: SELECT pool_state.qty FOR UPDATE; if qty < Q, RAISE EXCEPTION InsufficientInventory (matches §3.1 depletion semantics); otherwise UPDATE pool_state SET qty = qty - Q. unit_cost on the aggregate row mirrors standard_cost.unit_cost; it's redundant with the standard_cost table but kept for query consistency (queries against pool_state get a complete picture without joining standard_cost).

When standard_cost is revised, the new value applies to future trx_lines. Already-recorded trx_lines retain their C_std as recorded. The aggregate row's unit_cost is updated to the new standard at the next receipt or depletion (it's just a mirror; not a per-trx-history value).

Path C runs this exact logic for STD pools — no provisional-mode divergence applies.

### 3.4 Specific-id

Each unit is its own pool (pool.identity_key = unit_id, pool.method = 'specific'). The pool has one layer with qty=1 from its receipt; depletion consumes that one layer entirely. Same shape as FIFO with K=1.

**Specific-id pools always use strict mode, even on Path C.** Provisional mode exists to avoid layer iteration on layer-tracked methods; with K=1 there's only one layer to read, and the choice of which layer to consume is uniquely determined by the caller-provided identity_key (not by FIFO/LIFO ordering). The lock-hold reduction Path C exists to deliver doesn't apply — there's nothing to defer. The hot path under Path C for a specific pool runs identical SQL to a strict implementation: read the one layer, deplete it, INSERT trx_line with source_trx_line_id pointing at the receipt, DELETE the layer row. pool.provisional_basis is ignored for specific pools.

Application code must ensure specific pools receive exactly one receipt of qty=1 and never receive additional inflows. The schema enforces one piece of this invariant — `CHECK (method != 'specific' OR identity_key != 0)` on the pool table rejects specific pools with default identity_key, catching the configuration error where a caller forgot to supply a serial number. The schema does NOT enforce single-receipt or no-additional-inflow invariants; those remain application-layer responsibilities. Violation of the application-layer invariant produces undefined behavior under FIFO-with-K=1 semantics.

### 3.5 Provisional cost mode (Path C divergence for FIFO/LIFO)

Path C introduces **provisional mode** for FIFO and LIFO pools only. WAC pools run §3.1 strict; STD pools run §3.3 strict; specific pools run §3.4 strict; only FIFO and LIFO diverge into provisional mode.

Under provisional mode for FIFO/LIFO:

- **Receipts** behave identically to WAC's running-aggregate update (§3.1). The hot path writes only the layer_id=0 row of pool_state (the aggregate: qty, running average unit_cost). No per-layer pool_state rows are created on the hot path. The receipt's trx_line still records the actual receipt's qty and unit_cost (that's the source of truth for what arrived); the layer history is recoverable by scanning trx_line.
- **Depletions** compute applied_unit_cost from one of two sources, controlled by `pool.provisional_basis` (§2.2):
  - `'running_avg'` (default): use pool_state.layer_id=0.unit_cost — the running average maintained per the WAC formula.
  - `'standard'`: use standard_cost.unit_cost for the pool's (sku_id, location_id) — a standing standard cost maintained externally.
  The chosen value gets recorded as the depletion trx_line's unit_cost. `source_trx_line_id` is left NULL — the hot path does not commit to which historical receipt is being consumed; that determination happens during recalc/close.
- **Posting_lines** for depletions on the hot path book the provisional amount (qty × provisional_unit_cost) against the configured debit/credit accounts.

Recalc/close (deferred, §7) is the mechanism that later turns provisional costs into authoritative ones. It walks the trx_line stream per pool, runs strict FIFO/LIFO layer math, and posts cost-adjustment trxs for any variance against the provisional costs. The PoC does not implement recalc/close.

The hot path under provisional mode for a FIFO/LIFO pool produces the same SQL footprint as WAC under strict mode: one row update on pool_state at layer_id=0, no per-layer iteration, lock-hold time bounded by aggregate update rather than by number of layers consumed. **This is what the PoC measures.** For `'standard'` basis, the hot path additionally reads standard_cost (one constant-time index seek) but does not write it.

Both provisional_basis choices produce identical lock-hold characteristics; the basis choice affects only the magnitude of the variance that recalc/close (deferred) would later correct, not the hot-path performance profile.

### 3.6 Negative inventory (deferred extension)

**PoC behavior.** Depletions that would drive a pool's aggregate qty below zero are rejected via RAISE EXCEPTION InsufficientInventory (see §3.1 depletion, §3.4 specific). Aggregate qty in the PoC is therefore always ≥ 0. Receipts and depletions are the only paths that mutate aggregate qty, so this invariant holds for every method.

This is a deliberate scope cut. Real-world inventory systems often need to record events that drive on-hand qty negative — the physical good has left the warehouse before the receiving paperwork hits the ledger, or a customer-side transfer arrives before its corresponding shipment is recorded, or backflushing on a work-order completion consumes more than the on-hand record showed. ERPs handle this in different ways; the PoC's choice to reject all such cases keeps the spec simple and the measurements clean. Recording negative inventory is not on the critical path of what Path C is trying to prove (constant-time lock-hold for hot-path FIFO/LIFO).

**What enabling negative inventory would require.** Future production work is likely to need it. The implementation surface:

1. **Allow-negative gating.** Add a per-pool or per-trx-type flag controlling whether the InsufficientInventory check runs. Options:
   - `pool.allow_negative BOOLEAN DEFAULT FALSE` column. Per-pool decision; defaults to current behavior; can be turned on for pools that need the flexibility (e.g., transfer-shipment pools).
   - Per-trx-type override: specific trx_type values (e.g., `transfer_shipment`, `wo_backflush`) bypass the check regardless of pool setting.
   - Hybrid: pool-level default with per-trx override available.
   The hybrid is closest to production ERP behavior but adds spec complexity. Per-pool is simpler.

2. **WAC formula handling of negative-qty states.** When aggregate qty is allowed to go negative, the WAC running-average formula needs the `new_qty < 0` branch back. The previously-removed text covered this: preserve old_unit_cost when new_qty ≤ 0 (cannot compute meaningful average into an unreplenished short position). Zero-crossing replenishment (old_qty = -5, receipt Q = 15, new_qty = +10) becomes reachable; the formula `new_unit_cost = banker_div(old_qty × old_unit_cost + Q × C, new_qty)` works correctly for the math but the GL-level split-receipt treatment (booking the back-fill portion at old_unit_cost vs the accumulation portion at C) is a separate accounting concern that does not fit the single-aggregate-row model. Production deployments wanting GAAP-correct zero-crossing accounting would need either explicit split-receipt logic in plan_apply or a recalc/close pass that re-derives the correct GL postings.

3. **FIFO/LIFO under negative inventory.** Under Path C provisional mode, depletions read the running-average aggregate; the logic is the same as WAC's (preserve old_unit_cost when going negative, banker_div on receipt-to-positive). Recalc/close (deferred) would need to handle negative-layer states in its strict-mode reconciliation — likely by treating depletions-beyond-on-hand as "phantom layers" with a chosen cost basis until matching receipts arrive. This is a known hard problem in inventory accounting; SAP and Oracle solve it differently. Out of scope.

4. **Specific-id under negative inventory.** Specific pools have K=1 by construction; the concept of "negative specific inventory" doesn't apply (a serial-numbered unit either is on hand or isn't). Depletion of an already-depleted specific pool would remain an InsufficientInventory error even with allow-negative semantics enabled for other methods.

5. **GL implications.** Negative inventory has accounting implications beyond the ledger's perpetual record: PG-side aggregate qty going negative means the inventory account's debit balance can go negative (a credit balance, which is unusual for an asset account); some GAAP and IFRS treatments require reclassification or disclosure when this happens. The ledger itself doesn't enforce GL conventions; downstream reporting would need to interpret negative-aggregate states.

**Decision deferred to production design.** v3.1's PoC validates Path C's hot-path performance under the no-negative invariant. The choice of (a) which gating mechanism, (b) how to handle zero-crossing GL postings, and (c) how recalc/close reconciles negative-aggregate states is left to whichever future phase implements negative-inventory support.

### 3.7 Posting-account resolution (v3.2)

In the v3.1 PoC each SPI line carried `debit_account`, `credit_account` (and an optional `variance_account`), supplied verbatim by the caller and copied straight into `posting_line`. That pushed GL chart-of-accounts knowledge to every caller. v3.2 moves account resolution into the ledger: callers send only inventory facts (`pool_id, line_type, qty, unit_cost`), and the ledger looks the accounts up from a config table — mirroring how `standard_cost` is already resolved.

**Config table.** `posting_account_map` holds one full account set per `(sku_id, location_id)`:

```sql
posting_account_map(
  sku_id, location_id,                          -- PRIMARY KEY (no FK, like standard_cost)
  receipt_debit, receipt_credit,                -- po_receipt_line
  transfer_debit, transfer_credit,              -- transfer_shipment_line / transfer_receipt_line
  build_debit, build_credit,                    -- wo_output / wo_backflush
  scrap_debit, scrap_credit,                    -- wo_scrap
  adjustment_debit, adjustment_credit,          -- inv_adjustment_line / manual_adjustment_line
  revaluation_debit, revaluation_credit,        -- revaluation_line
  variance_acct                                 -- STD purchase-price variance; NULL if never STD
)                                               -- account columns FK account(id)
```

**Direction.** Each operation's pair is stored in the **receipt (inventory-increase) direction**: `debit` = the pool's inventory side, `credit` = the contra. The cost engine uses the pair as-is for receipts (`qty > 0`) and **swaps it** (debit ↔ credit) for depletions (`qty < 0`). One uniform rule covers every operation — a `transfer_shipment_line` depletion swaps the transfer pair to `(debit contra, credit inventory)`; an `inv_adjustment_line` loss swaps the adjustment pair. The engine owns direction; the config stores it once.

**line_type → operation.** A fixed code-level map selects which column pair applies: `po_receipt_line → receipt`; `transfer_{shipment,receipt}_line → transfer`; `wo_output`/`wo_backflush → build`; `wo_scrap → scrap`; `inv_adjustment_line`/`manual_adjustment_line → adjustment`; `revaluation_line → revaluation`. Adding a fundamentally new operation is an `ALTER TABLE` adding a column pair — appropriate when the transaction taxonomy itself changes, not per SKU.

**STD variance leg (§3.3).** For an STD receipt whose actual cost differs from standard, the variance leg flips between `variance_acct` and the receipt-direction `credit` (the contra/source), favorable vs unfavorable. If `variance_acct` is NULL for that pool, RAISE EXCEPTION (`MissingVarianceAccount`).

**Hydration + fail-loud.** `posting_account_map` is joined on the pool's `(sku_id, location_id)` and hydrated into the snapshot for every touched pool (every line posts a journal row), alongside `standard_cost` under the same `pool_lock`. A touched pool whose `(sku_id, location_id)` has no row fails loud: RAISE EXCEPTION (`MissingPostingAccounts`) — same posture as a missing `standard_cost`. There is no caller-supplied fallback.

This is GL-routing configuration off the hot-path locking critical section; it does not change the lock-hold or aggregate-update profile the PoC measures (one extra constant-time index seek per touched pool at hydration, like the `standard_cost` join).

## 4. SPI surface

Path C exposes two flavors of hot-path entry, corresponding to direct and routed shapes:

```rust
ledger_submit_trx_c(
    trx_type: trx_type,
    source_id: BIGINT,
    posted_at: TIMESTAMPTZ,
    lines: ARRAY of (line_type, source_id, pool_id, qty, unit_cost)
) RETURNS BIGINT  -- trx.id (direct flavor)

ledger_enqueue_trx_c(
    trx_type: trx_type,
    source_id: BIGINT,
    posted_at: TIMESTAMPTZ,
    lines: ARRAY of (line_type, source_id, pool_id, qty, unit_cost)
) RETURNS BIGINT  -- submission_id (routed flavor; shmem-local, not trx.id)
```

> **Implementation note (v3.1 PoC — AUDIT.md D1.1/D1.2).** The shipped SPIs take `lines` as
> **JSONB** (an array of objects), not the SQL composite ARRAY sketched above — pgrx ergonomics;
> behaviorally equivalent.

> **v3.2 — posting accounts are resolved ledger-side (§3.7), not on the line.** The original
> v3.1 line tuple carried `debit_account`, `credit_account` (and an optional `variance_account`)
> supplied verbatim by the caller. v3.2 drops all three: the ledger resolves debit/credit (and the
> STD variance account) from `posting_account_map` keyed on the touched pool's `(sku_id,
> location_id)`, hydrated at lock time like `standard_cost`. Callers no longer need to know the GL
> chart of accounts. A touched pool with no `posting_account_map` row fails loud (§3.7). This
> changes the SPI wire contract (the tuple above is the v3.2 shape) but not what the v3.1 PoC
> measured — it is GL-routing config, off the hot-path-locking critical section.

Both flavors are in PoC scope. Direct Path C demonstrates per-caller-tx provisional cost recording with reduced lock-hold time vs strict-mode paths. Routed Path C demonstrates the same provisional cost handling under batched-commit semantics — the combination that the hot-pool deep-FIFO regime needs. The two flavors map onto the same matrix that strict direct vs strict routed does (low/high concurrency × disjoint/overlapping pools); Path C's value at the architecturally-interesting cell (high concurrency, hot FIFO/LIFO pools) is fully realized only with routed.

## 5. Path C direct flavor (ledger_submit_trx_c)

Caller invokes inside their own user-tx, alongside whatever other work they're doing. The function does the full ledger work synchronously.

### 5.1 Function logic

1. Compute the set of pool_ids touched. Sort ascending, dedup.
2. Acquire locks in singleton-loop sorted order on pool_lock. For each pool, the optimistic pattern is: `SELECT 1 FROM pool_lock WHERE pool_id = $1 FOR UPDATE` — one SPI in the steady state where the pool_lock row already exists. If that SELECT returns zero rows (pool_lock not yet created for this pool — happens once per pool over its lifetime), lazy-create with `INSERT INTO pool_lock (pool_id) VALUES ($1) ON CONFLICT DO NOTHING` and re-issue the SELECT FOR UPDATE. The retry path is two extra SPI calls per pool, but only on its first-ever touch; the steady state is one SPI per pool. **All locks must be held before reading pool_state in step 3** — otherwise concurrent callers could read inconsistent aggregate state and produce racy updates.
3. Bulk-read per-pool routing info and aggregate state for all touched pools:
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

4. For any pool with `method = 'std'` OR (`method IN ('fifo','lifo')` AND `provisional_basis = 'standard'`), bulk-read standard_cost:
   ```sql
   SELECT p.id AS pool_id, sc.unit_cost AS standard_unit_cost
     FROM pool p
     JOIN standard_cost sc ON sc.sku_id = p.sku_id AND sc.location_id = p.location_id
    WHERE p.id = ANY($2::bigint[])  -- subset that needs standard cost
   ```
   If a pool in the subset has no matching standard_cost row, RAISE EXCEPTION (configuration error).

5. For any pool with `method = 'specific'`, bulk-read its single layer row:
   ```sql
   SELECT pool_id, layer_id, qty, unit_cost
     FROM pool_state
    WHERE pool_id = ANY($3::bigint[]) AND layer_id > 0  -- subset of specific pools
    ORDER BY pool_id, layer_id
   ```
   For FIFO/LIFO under provisional mode, no layer rows exist on the hot path — this query is skipped. For specific pools (which use strict mode even on Path C, §3.4), this returns the one layer row per pool.

6. Call ledger-core dispatching per pool's method:
   - `method = 'wac'`: plan_apply_wac (strict; identical to a strict-mode implementation).
   - `method = 'std'`: plan_apply_std using standard_unit_cost looked up in step 4; receipts emit variance posting_lines per §3.3.
   - `method = 'specific'`: plan_apply_specific (strict; consumes the one layer row).
   - `method = 'fifo'` or `'lifo'`: plan_apply_provisional. Receipts update aggregate per the §3.1 WAC formula (with §3.1 divide-by-zero guard and §3.0 banker's rounding on the division). Depletions get applied_unit_cost from either `aggregate.unit_cost` (if provisional_basis = 'running_avg') or `standard_unit_cost` (if provisional_basis = 'standard'). new_qty = old_qty - Q on aggregate. trx_line.source_trx_line_id = NULL.

7. On error (insufficient inventory for any pool — checked against aggregate.qty for WAC/FIFO/LIFO provisional **and STD** (STD maintains the aggregate row for qty tracking per §3.3, so the no-negative-inventory invariant of §3.6 applies to every method), against the one layer for specific): RAISE EXCEPTION, caller's tx aborts.

8. Apply PlanResult:
   - INSERT trx ... RETURNING id (PG auto-allocates the trx.id via the IDENTITY column; capture it for use in trx_line.trx_id below and for the return value).
   - Bulk INSERT trx_line rows (each row's trx_id set to the just-allocated trx.id; trx_line.id auto-allocated by IDENTITY).
   - Bulk UPSERT pool_state (aggregate row updates for all methods that maintain it; layer row mutations for specific pools).
   - Bulk INSERT posting_line rows (posting_line.id auto-allocated).
9. Return the trx.id captured in step 8.

Caller's user-tx commits. One fsync.

### 5.2 Lock-hold properties

For FIFO/LIFO pools under provisional mode, lock-hold time per trx is bounded by aggregate-row work only — no layer iteration. For a hot SKU under FIFO with hundreds of historical layers, Path C holds the lock for the same duration as a WAC update — orders of magnitude less than a strict-mode implementation would. This is what the PoC measures.

For WAC, STD, and specific pools, Path C's lock-hold characteristics match a strict-mode implementation's (specific has K=1 so iteration cost is constant; WAC has no layers; STD has no *layer-row* mutation — it UPSERTs the aggregate `(pool_id, layer_id=0)` row on every receipt and depletion, exactly like WAC, per §3.3).

### 5.3 Per-trx SPI count

Per-trx SPI count for Path C direct, dominant case (FIFO/LIFO pool, P deduped pools, all pool_locks already created):
- 1 INSERT trx (with RETURNING id).
- P `SELECT 1 FROM pool_lock WHERE pool_id = $1 FOR UPDATE` (one per pool, steady state).
- 1 SELECT pool + pool_state aggregate join.
- 0-1 SELECT standard_cost (only if any 'standard'-basis or STD pools are touched).
- 0-1 SELECT pool_state layer rows (only if any specific pools are touched).
- 1 bulk INSERT trx_line (with RETURNING id if posting_line needs it).
- 1 bulk UPSERT pool_state.
- 1 bulk INSERT posting_line.

Total steady state: ~5-7 bulk SPI + P singleton. For P=1: ~6-8 SPI. The throughput difference vs strict-mode comes from reduced lock-hold time on FIFO/LIFO depletions, not from reduced SPI count.

First-time-pool overhead: +2 SPI per pool that hasn't been locked before in its lifetime (INSERT ON CONFLICT + re-SELECT FOR UPDATE). Amortized to zero in steady state.

### 5.4 Failure handling

Path C direct fails the entire submission on any error within the function. The caller's user-tx aborts via RAISE EXCEPTION; no trx row, no trx_line rows, no posting_line rows are written. PG handles the rollback. Caller observability: the SQL exception propagates back through the caller's session.

### 5.5 Caller-side batching via standard PG transactions

A caller with multiple trxs to submit can wrap N `ledger_submit_trx_c` calls in a single user-tx, getting a meaningful subset of the routed flavor's batching benefit without any new SPI surface. Standard PG semantics deliver:

- **One fsync at COMMIT** for all N trxs, not N fsyncs.
- **Pool_locks held across the batch** — once a pool_lock row is FOR UPDATE-acquired in submission #1, submissions #2..N that touch the same pool re-acquire it as a no-op (PG tracks per-tx row locks).
- **Contiguous trx_line.id allocation** — the identity column allocates ids sequentially within the tx.
- **One commit-time WAL write** for the whole batch.

What the caller still pays per-submission inside the tx:
- One SPI roundtrip per `ledger_submit_trx_c` call.
- Per-call hydration of pool/pool_state (each call reads state fresh from the database, not from a shared in-memory snapshot).
- Per-call plan_apply_provisional invocation in ledger-core.

The architectural distinction: caller-batched direct flavor amortizes commit cost (fsync, WAL) and within-tx lock-acquisition cost across N trxs, but it doesn't aggregate work across DIFFERENT callers. 1000 concurrent callers each batching 10 trxs of their own still produces 1000 user-txs hitting pool_lock serially. The routed flavor's distinct value is cross-caller aggregation — collapsing 1000 callers' work into one commit_group via shmem.

The PoC measures both: per-call direct, caller-batched direct, and routed (§10.6, §11). The crossover analysis distinguishes how much of routed's win comes from batching mechanics vs from cross-caller aggregation.

## 6. Path C routed flavor (ledger_enqueue_trx_c)

Routed Path C uses shmem-staged submissions and background committer workers, similar to a generic routed architecture. This section specifies the full routing infrastructure inline so v3.1 is implementable without external documents.

### 6.1 Submission

```rust
ledger_enqueue_trx_c(...) RETURNS BIGINT  -- submission_id
```

Caller invokes inside their own user-tx (or as a standalone call). The function:

1. Pushes a descriptor (trx_type, source_id, posted_at, pool_ids touched, line payload, caller's user_tx_xid) to the shmem staging queue. Receives a shmem-local submission_id.
2. Returns submission_id.

No DB write at submission. The trx row is created only when the committer successfully processes the submission. If the committer fails or the postmaster crashes before processing, no trx row exists and the submission is lost — caller can resubmit. The submission_id is meaningful only for the shmem queue's lifetime.

Caller observability (polling for completion, knowing trx.id, error reporting) is out of scope for this PoC. The PoC measures committer throughput, not caller-side API design.

**Harness polling for outcome.** The benchmark harness determines per-submission outcome by polling the trx table by the submission's (trx_type, source_id) pair: `SELECT id FROM trx WHERE trx_type = $1 AND source_id = $2`. Present → recorded. Absent after a configurable timeout → failed-or-still-pending (the harness counts both as "did not complete within window"; distinguishing the two requires the kind of caller observability that's deferred). This is sufficient for measuring throughput, success rates, and tail latencies; it is NOT sufficient for production error reporting.

### 6.2 Shmem layout

The routed path needs shmem for the router and committer coordination:

- `staging_queue`: ring buffer of StagingEntry structs. Each entry holds trx_type, source_id, posted_at, caller user_tx_xid, pool_id list, line payload (variable-length, in arena).
- `committer_queue`: ring buffer of CommitterQueueEntry structs. Each entry represents a commit_group: a list of staging entries grouped by pool overlap.
- `spillover_arena`: variable-length payload storage (line arrays, pool_id arrays).
- `committer_identity_registry`: extension-owned shmem array tracking active committer BGWorkers. Each slot in the array holds: (a) the committer's claim token, a monotonically-increasing uint64 incremented on every committer startup, (b) the committer's BGWorker pid, (c) status flag (active/dead/empty). On startup, a committer atomically claims a free slot via CAS and writes its (token, pid). When other workers need to check committer liveness, they look up the registered pid and probe it (via `kill(pid, 0)` or PG's `pg_stat_activity` for the worker's backend); a dead pid means the committer crashed. The (slot, token) tuple is recorded in each `CommitterQueueEntry` that the committer claims, so recovery can identify which committer was working on which commit_group. Slot reuse is safe because the monotonic token differentiates restarts — a recovering worker that sees an old (slot, token) in a queue entry knows it belongs to a now-dead committer instance even if the slot has been reclaimed by a new live committer.

State machines and CAS ordering for staging entries:
- `empty` (0): slot is free.
- `pending` (1): submitted, awaiting routing.
- `processing` (2): router has claimed; in the middle of being stamped into a commit_group.
- `routed` (3): router has committed the entry to a commit_group; committer can claim.
- `in_flight` (4): committer has claimed the commit_group and is processing.

Transitions: caller `empty → pending`; router `pending → processing → routed`; committer reads `routed` entries via its commit_group claim; on successful commit, committer transitions entries `routed → empty` and frees arena space.

State machine for committer_queue entries (each entry represents one commit_group):
- `empty` (0): slot is free.
- `ready` (1): router has assembled the commit_group and pushed it here; awaiting committer claim.
- `in_flight` (2): a committer has claimed (CAS ready → in_flight) and is processing; the entry records the claiming committer's (slot, token) for recovery.
- `done` (3): committer's PG tx committed successfully; awaiting cleanup.

Transitions: router `empty → ready`; committer `ready → in_flight`; committer (on successful commit) `in_flight → done → empty` (the last transition runs in cleanup step §6.4 step 12). On committer death, recovery walks the queue looking for `in_flight` entries whose claiming committer is no longer alive, and reclaims them via CAS to a new live committer.

### 6.3 Router

Background worker on a latch loop with a 50ms timeout (`wait_latch(Some(50ms))`, `ledger-routed-c/src/router.rs`) — the 50ms tick, **not** `batch_window_us`, is the scan cadence. Each tick:

1. Head-scan up to `router_window_size` (default 1000) pending entries from the staging queue, skipping entries in eject cooldown (`eject_count > 0 AND now - last_eject_at_ns < eject_cooldown_ms`, default 10ms).
2. **Time-coalesce gate** (`batch_window_us`, default 500μs). If the oldest scanned candidate has been pending for less than `batch_window_us`, defer the whole tick (emit nothing) so more submissions accumulate before grouping; `batch_window_us = 0` disables the gate. This is a coalesce *dwell* applied per tick — not the scan cadence.
3. Build union-find by pool_id overlap. Each connected component becomes a candidate commit_group.
4. **Disjoint-packing pass** (`router_pack_disjoint`, production default ON — acct-p1al). Greedily first-fit the disjoint components (those sharing no pool_id) into commit_groups of up to `batch_size_max`, so a spread/Pareto tick emits a few fuller groups instead of many tiny one-component groups, amortizing per-group commit/fsync. Disjoint pools share no row lock, so this adds no cross-pool `FOR UPDATE` contention. Off → one commit_group per component.
5. Chunk any group exceeding `batch_size_max` (default 200 submissions) into batch-sized chunks (cross-chunk ordering posture: §14.2).
6. For each commit_group: CAS staging entries `pending → processing → routed`; push the commit_group to the committer queue.

Affinity grouping ensures overlapping submissions go to the same committer — one connected component becomes one commit_group claimed by one committer, avoiding committer-vs-committer lock contention on hot pools. (This routing-level grouping is separate from the experimental `affinity_scheme` committer-pinning dial, default off — acct-0usf.)

### 6.4 Committer

Pool of BGWorkers (default 4). Each:

1. Claim a commit_group via CAS on committer queue entry (`ready → in_flight`). The shmem entry records the claiming committer's identity (slot + token from CommitterIdentityRegistry, see §6.5).
2. Open a PG tx (top-level, READ COMMITTED).
3. Read the commit_group's submissions and lines from shmem.
4. Check pg_xact_status for each submission's caller user_tx_xid. `pg_xact_status(xid)` is a PG system function that returns `'committed'`, `'aborted'`, or `'in progress'` for a given transaction id; it reads from PG's clog (transaction commit log, persistent and always available — independent of `track_commit_timestamp`):
   - 'committed': keep.
   - 'aborted': drop the submission from the batch (no trx will be created for it).
   - 'in progress': eject (CAS staging entry back to `pending`, increment eject_count, store last_eject_at_ns, exclude). The cooldown prevents tight cycling.
   - *no status returned* (NULL — e.g. the xid predates clog truncation): treat as Unknown and keep (optimistic, bounded by the caller-tx eject timeout).
   - *any other, unrecognized status string*: treat as not-yet-committed — emit a WARNING and eject (never keep). This defends against a future PG wording change silently reclassifying an in-progress caller as keep-able. (acct-yojk.10: `CallerTxStatus::Unrecognized`, distinct from the NULL → Unknown case.)

5. **Pre-flight dedup against trx.** Bulk-read existing trx rows matching the batch's (trx_type, source_id) pairs:
   ```sql
   SELECT trx_type, source_id FROM trx
    WHERE (trx_type, source_id) IN (... batch's pairs ...);
   ```
   Any submission whose (trx_type, source_id) is already in trx is a duplicate of a previously-recorded receipt. Exclude these submissions from the commit_group immediately. The trx table's UNIQUE constraint remains as a structural backstop, but the committer doesn't depend on it firing for happy-path control flow.

   **What this dedup defends against.** Three scenarios produce a duplicate (trx_type, source_id):
   1. **Caller resubmission after committer death.** The original committer wrote the trx row, then died. Caller re-submits because polling hasn't seen the result yet. The recovering committer (or any later committer) finds the original trx via this dedup and skips re-processing.
   2. **Recovery committer re-processing an aborted commit_group.** If the original committer claimed the commit_group and partially processed it (e.g., wrote some trx rows but died before COMMIT), PG rolled the partial work back — so trx rows for that commit_group don't exist and the recovery committer processes fresh. But if the original committer DID commit successfully and died after, the trx rows DO exist; recovery reads them via this dedup and skips.
   3. **Caller-side double-submission bug.** Application code submits the same (trx_type, source_id) twice. The dedup catches the second one cleanly; without dedup, the second would hit the UNIQUE constraint at INSERT time and abort the entire commit_group (per §6.8 error handling).

   **What this dedup does NOT fully defend against — the cross-commit-group same-key race.** A *single* submission is never processed by two committers concurrently: each staging entry is claimed by exactly one committer via CAS on the entry's state, and the router places each submission in exactly one commit_group. But two *distinct* submissions that happen to carry the same (trx_type, source_id) — the caller-side double-submission of scenario 3 — are not guaranteed to land in the same commit_group. Commit_groups are formed by pool overlap (§6.3), not by (trx_type, source_id); so if the two submissions touch disjoint pools they route to *different* commit_groups, and the within-batch dedup above never compares them. Two committers can then process those commit_groups concurrently and both reach INSERT with the same key — one wins, the other gets a 23505. That race (and the recovery-after-committer-death case) is caught by §6.8's re-drive-on-UNIQUE, not by this pre-flight dedup. The pre-flight dedup is the steady-state fast path; the §6.8 re-drive is the race backstop.

6. Compute the union of pool_ids across included submissions (after dedup). Sort ascending, dedup.
7. Acquire pool_lock FOR UPDATE in singleton-loop order using the same optimistic pattern as §5.1 step 2: `SELECT 1 FROM pool_lock WHERE pool_id = $1 FOR UPDATE` per pool, with a lazy-create retry path (`INSERT INTO pool_lock (pool_id) VALUES ($1) ON CONFLICT DO NOTHING` followed by re-SELECT) only if the pool's lock row doesn't yet exist.
8. Bulk-read per-pool routing info, aggregate state, and (where needed) standard_cost and specific layer rows — same queries as direct flavor §5.1 steps 3-5 but bulk-keyed across all pools in the commit_group.
9. Process submissions in submission_id (enqueue) order. For each submission, dispatch to ledger-core per pool's method (§5.1 step 6's dispatch). Apply results to a working snapshot.

   **Failure handling within the commit_group**: a submission that fails (e.g., depletion exceeds aggregate qty given the working snapshot's state at the moment it's processed) is excluded from the working snapshot. Remaining submissions continue using the snapshot. **No pristine-snapshot replay is needed** because Path C has no cross-trx state dependency — failed submissions contribute nothing to the snapshot, so there's nothing to back out (see §14.2 for full discussion of ordering semantics, including why submission ordering matters and is preserved by enqueue order rather than reordered to maximize success). Failed submissions produce no trx row.

10. After clean pass: bulk INSERT trx ... RETURNING id (one row per included submission; capture returned IDs to populate trx_line.trx_id and posting_line.trx_line_id), bulk INSERT trx_line (also RETURNING id for posting_line.trx_line_id), bulk UPSERT pool_state (aggregate rows for all methods that maintain them; layer rows for specific pools), bulk INSERT posting_line.
11. COMMIT. One fsync.
12. Cleanup: CAS the committer_queue entry `in_flight → done → empty` (or directly `in_flight → empty` if the intermediate `done` state is not needed for recovery liveness). CAS each constituent staging entry `routed → empty`. Free arena blocks held by the commit_group's payloads.

One fsync per commit_group. With N submissions per commit_group, fsync cost amortizes by N.

### 6.5 Recovery

- **Router death**: shmem boot sweep on router restart. Staging entries at `processing` get inspected: if their CommitterQueueEntry doesn't exist or is at empty, revert to `pending` via CAS.
- **Committer death**: CommitterIdentityRegistry tracks active committers via (slot, token); liveness is checked by probing the registered pid. When a committer's pid is dead, any committer_queue entry it claimed (state = `in_flight`, slot/token matching the dead committer's registration) is reclaimable. A live committer reclaims via CAS that swaps the entry's (slot, token) for its own, leaving state at `in_flight`. The reclaiming committer then runs §6.4 from step 2 normally. Step 5's pre-flight dedup against trx (`SELECT trx_type, source_id FROM trx WHERE (trx_type, source_id) IN (...)`) serves as the recovery source of truth: any submissions the dead committer already wrote to trx are skipped via this dedup; remaining submissions are processed fresh. The trx UNIQUE constraint guarantees no duplicate trx rows can be created even if the dead committer's PG tx committed and the recovery committer raced concurrently.
- **Postmaster crash**: shmem is gone. All in-flight submissions are lost. No trx rows exist for them. Callers observe the loss (their polling never sees a trx for that source) and resubmit.

### 6.6 Per-commit_group SPI count

For a commit_group with N submissions touching P deduped pools (steady state, all pool_locks pre-existing):
- 1 pre-flight dedup SELECT against trx.
- P `SELECT 1 FROM pool_lock WHERE pool_id = $1 FOR UPDATE` (one per pool).
- 1 bulk pool+pool_state aggregate read.
- 0-1 bulk standard_cost read.
- 0-1 bulk pool_state layer-row read (specific pools only).
- 1 bulk INSERT trx, 1 bulk INSERT trx_line, 1 bulk UPSERT pool_state, 1 bulk INSERT posting_line.

Total steady state: ~9 bulk + P singleton. For N=50 submissions at P=50 deduped pools: ~59 SPI per commit_group, amortizing to ~1.18 SPI/submission. The architectural win vs direct flavor: at high concurrency on overlapping pools, the per-batch pool_lock acquisition and aggregate UPDATE replace what direct flavor would do per-trx — a 1000:1 reduction in pool_lock acquisitions for the hot-pool worst case.

First-time-pool overhead: +2 SPI per pool that hasn't been locked before in its lifetime (lazy-create path, §5.1 step 2). Amortized to zero in steady state.

### 6.7 Batching benefit on hot pools

The architectural win Path C routed provides over Path C direct: 1000 concurrent submissions to one hot FIFO pool become one commit_group, processed in one PG tx, with one pool_lock acquisition and one aggregate UPDATE encompassing the batch's combined delta. Direct flavor would serialize 1000 individual pool_lock acquisitions and 1000 individual aggregate UPDATEs.

For `'standard'`-basis FIFO/LIFO pools, batching is even cleaner — standard_cost is read once at hydration, every depletion in the batch uses it, no within-batch state evolution. Pure data shuffle. For `'running_avg'`-basis pools, the running average evolves through the batch as receipts arrive in the batch's processing order; the recorded provisional cost for a depletion thus depends on within-batch ordering. That's different from direct flavor's per-tx evolution but equally well-defined; any within-batch ordering "errors" are corrected by recalc/close.

### 6.8 Committer SQL error handling

The committer's PG tx can fail mid-flight from causes other than per-submission plan_apply errors. Per-submission errors (e.g., InsufficientInventory, MissingStandardCost) are handled in step 9 by dropping the offending submission from the working snapshot and continuing — these don't abort the tx. SQL-level errors do abort the tx and must be classified into transient (retry) vs fatal (poison):

**Transient errors — abort tx, retry the commit_group.**
- Deadlock (SQLSTATE 40P01): PG detected a deadlock and aborted this tx. Retry the commit_group with exponential backoff. The pool_lock acquisition order (sorted by pool_id) makes deadlocks unlikely between committers, but cross-tx interactions (e.g., a vacuum holding a relation lock) can still produce them.
- Serialization failure (SQLSTATE 40001): if a deployment runs committers under SERIALIZABLE isolation (the PoC uses READ COMMITTED, so this shouldn't fire), PG may abort with a serialization conflict. Retry.
- Lock-wait timeout: the committer's `SELECT FOR UPDATE` on pool_lock blocked too long. Retry after backoff.
- Connection drop / network blip during PG roundtrip: retry after re-establishing the connection.

Retry mechanics: the committer leaves the commit_group entry in shmem at `in_flight` with its own (slot, token), backs off (start 10ms, exponential up to 1s, max 5 retries), and resumes from §6.4 step 2 (open new PG tx, repeat pg_xact check, repeat dedup, etc.). Pre-flight dedup catches any work that other committers may have completed in the interim. After max retries with no progress, the commit_group is marked poisoned (see below).

**Unique-violation that survives pre-flight dedup — re-drive the group minus the offender.** A 23505 on `trx(trx_type, source_id)` at INSERT time means a racing committer committed one of this group's keys between our pre-flight dedup and our write (two duplicate submissions that landed in different commit_groups — different pools — and so weren't caught by within-batch dedup). The offending key is now visible in `trx`, so the committer re-runs dedup against `trx`, drops the now-resolvable duplicate(s), and re-drives the rest of the group through the retry loop (a fresh hydrate + replan recomputes the aggregate without the offender). This avoids dead-lettering the group's innocent siblings alongside the one duplicate. If the re-dedup resolves no offender (a UNIQUE other than `(trx_type, source_id)`, or no duplicate is actually present), re-driving can't make progress and the group is poisoned as a genuine fatal. (Implemented: `committer.rs` `PhaseOutcome::DuplicateRace`; the re-drive count and the irresolvable-poison are both counted under `committer_duplicate_redrives_total`. acct-yojk.9.)

**Fatal errors — poison the commit_group, do not retry.**
- Type errors, NULL constraint violations on required columns: programming errors. Retrying won't help.
- Disk full (SQLSTATE 53100): retrying is futile until operator intervenes.

Poisoning marks the commit_group entry with a `poisoned` state in shmem (an extension of the queue entry state machine in §6.2; alternative is a sidecar log table). Poisoned commit_groups don't get re-claimed by recovery. Their constituent submissions are lost from the caller's perspective (no trx row appears, polling times out); production deployments would log poisoned commit_groups for operator review. The PoC counts poisoned commit_groups as a harness metric.

**The PoC's posture.** The PoC implements retry-on-deadlock as a basic resilience measure. More sophisticated error classification (per SQLSTATE) is a production hardening concern. The harness measures: (a) deadlock-induced retry count per workload, (b) successful-after-retry rate, (c) poisoned commit_group count. If deadlock retries dominate committer time under any workload, that's a real PoC finding (the lock-acquisition ordering isn't preventing deadlocks under that load).

## 7. Recalc / close (deferred)

This PoC does not implement the mechanism that turns provisional FIFO/LIFO costs into authoritative ones. Authoritative cost reconciliation — call it recalc, settlement, or period close depending on the model — is out of scope. See §13.

What it would do, in broad strokes: walk the trx_line stream for each layer-tracked pool in chronological order (allocation order via trx_line.id, business-effective time via JOIN to trx.posted_at, or a derived Cost Date), run strict FIFO/LIFO layer math, and post cost-adjustment trxs and trx_lines for any variance between the recomputed authoritative cost and the provisional cost recorded on the hot path. Materialized layer rows under pool_state at layer_id > 0 would also be written.

Implementation options that production ERPs use:

- **Oracle-style continuous worker**: a background worker pool runs a per-pool replay continuously. Variance is posted as soon as it's detected. Mid-period queries see provisional costs until the worker catches up.
- **SAP-style on-demand**: caller invokes `ledger_settle_pool(pool_id)` before a statutory query. Forces synchronous reconciliation.
- **Dynamics-style periodic close**: an Inventory Close job runs on schedule (nightly, end-of-period), reconciles everything, gates the period transition on completion.

Each has different tradeoffs (lag, resource consumption, predictability) and different schema requirements: the continuous worker needs progress tracking (a watermark on pool, or a settled-state column on trx_line, or full-recompute idempotency); the on-demand mode needs an SPI entrypoint; the periodic close needs the accounting_period table's close hook.

When recalc/close is added, the schema additions are likely to include: a `cost_adjustment` value in the trx_type / line_type / posting_event_type enums; a `cost_adjustment_id_seq` sequence for unique trx.source_id on adjustment trxs; possibly a denormalized `trx_line.posted_at` for business-time ordering; possibly a `total_value` column on pool_state for variance-into-empty-pool absorption; possibly watermark columns on pool or a `settlement_state` column on trx_line. None of those columns or enum values are in the PoC schema.

**Strategic risk acknowledgment.** Path C's value proposition depends on a working recalc/close mechanism. If reconciliation turns out to be impractical at scale, Path C merely shifts the bottleneck from hot path to batch window. SAP CKMLCP, Oracle Cost Processor, and Dynamics Inventory Close all run at high transaction volumes in production, which is empirical evidence that the pattern is feasible — but those production validations don't substitute for measuring recalc/close on this specific schema. The decision to defer is a scope decision, not a feasibility judgment.

The PoC does not pre-commit to any one of these implementation options. The choice of worker model (continuous / on-demand / periodic), progress-tracking mechanism (watermark column on pool, settled-state column on trx_line, full-recompute idempotency, txid_snapshot-based safe-watermark), and schema additions is made when recalc/close is built — informed by what the production workload demands and by what the v3.1 PoC's hot-path measurements show.

## 8. ledger-core (shared Rust crate)

ledger-core is a pure-Rust crate containing the per-method state transitions and the dispatch logic. No pgrx dependency. Unit-testable in isolation.

```
ledger-core/
  src/
    method.rs                  - PoolMethod enum, plan_apply trait
    wac.rs                     - WAC plan_apply (strict; uses numeric::banker_div on receipts)
    std.rs                     - STD plan_apply (strict; emits variance posting_lines)
    specific.rs                - Specific-id plan_apply (strict)
    fifo.rs                    - stub returning MethodMismatch error in PoC scope;
                                 full strict implementation added by future
                                 recalc/close work (out of scope here)
    lifo.rs                    - stub returning MethodMismatch error in PoC scope;
                                 same as fifo.rs
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
                                 (InsufficientInventory, MethodMismatch,
                                  MissingStandardCost, etc.)
```

The two pgrx extensions implementing Path C (`ledger-direct-c` for direct flavor, `ledger-routed-c` for routed flavor) both depend on ledger-core. They invoke its functions with the same Snapshot types and consume the same PlanResult outputs. Differences are in how they get the snapshot (direct: hot-path SQL in caller's tx; routed: hot-path SQL in committer's tx after assembling the commit_group from shmem).

fifo.rs and lifo.rs are stubs in the PoC because Path C's hot path never invokes them — the provisional dispatch in provisional.rs handles all FIFO/LIFO calls. The stubs exist for two reasons: (a) the per-method dispatch in plan_apply needs entries for all PoolMethod enum variants to typecheck; (b) a future recalc/close phase will need real strict implementations and will replace the stubs at that time. If a caller dispatches to fifo.rs or lifo.rs in PoC scope, that's a configuration bug (Path C shouldn't be calling strict layer math) and the MethodMismatch error correctly surfaces it.

## 9. Testing strategy

### 9.1 ledger-core unit tests

For numeric::banker_div (§3.0):
- Exact division (no remainder): returns the quotient unchanged.
- Below-half remainder: rounds toward zero.
- Above-half remainder: rounds away from zero (toward +inf for positive, -inf for negative).
- Exactly-half cases: rounds to nearest even — verify both `q even → q` and `q odd → q ± 1` cases.
- Negative numerator, positive denominator: same rounding rules with sign tracking.
- Both signs negative: same.
- Bias check: sum of banker_div(n, 2) for n in 0..1000 differs from sum of (n/2) by at most O(1), demonstrating bias cancellation vs always-truncate.
- **Numerator near i128 limits**: banker_div((i128::MAX / 2), 3) and similar inputs that exercise full-range i128 arithmetic; result must equal mathematical truth.
- **Overflow on downcast**: banker_div(i64::MAX as i128 * 2, 1) should panic (true quotient exceeds i64); verify the panic surfaces rather than silently wrapping.
- **WAC-formula multiplication overflow regression**: simulate the formula `old_qty × old_unit_cost + Q × C` with values that overflow i64 but fit in i128 (e.g., qty = 10^10, unit_cost = 10^9 in 1e-6 BIGINT-units representing $1000 × 10^10 units). Verify banker_div invoked with proper i128 casting produces correct result; verify that omitting the cast (callsite bug) would be caught by the type signature at compile time.

For plan_apply (strict mode, used by Path C for WAC/STD/specific):
- WAC: single receipt, single depletion, mixed-cost receipts, defensive-guard test (degenerate qty=0 receipt against qty=0 pool returns no-op without panic; non-PoC scenario but the guard is exercised), depletion exceeding qty raises InsufficientInventory (no-negative-inventory invariant per §3.6).
- STD: receipt emits variance posting_line, depletion at standard cost, missing standard_cost raises MissingStandardCost.
- Specific: single receipt of qty=1, depletion consumes layer entirely, second depletion on same pool raises InsufficientInventory.

For plan_apply_provisional (Path C hot path for FIFO/LIFO):
- Receipts update aggregate per WAC formula (with banker_div rounding).
- Depletions with 'running_avg' basis use aggregate's running unit_cost.
- Depletions with 'standard' basis use standard_cost.unit_cost.
- Missing standard_cost for 'standard'-basis pool raises MissingStandardCost.
- source_trx_line_id is NULL on all depletion trx_lines.
- No pool_state layer rows are mutated.

### 9.2 ledger-direct-c integration tests

Run against a real PostgreSQL with the schema installed.

- Submit FIFO receipt + depletion via ledger_submit_trx_c, verify aggregate updated correctly, trx_line records provisional cost, no layer_id > 0 pool_state rows created.
- Submit WAC receipt + depletion via ledger_submit_trx_c, verify behavior identical to a strict-mode WAC implementation.
- Submit STD receipt via ledger_submit_trx_c with a standard_cost row present, verify trx_line at standard cost and variance posting_line for the actual-vs-standard delta.
- STD pool with no standard_cost row: verify RAISE EXCEPTION at hot-path time.
- Submit specific receipt + depletion via ledger_submit_trx_c, verify identical SQL footprint to strict mode (specific bypasses provisional mode, §3.4).
- FIFO/LIFO pool with `provisional_basis = 'running_avg'`: depletion records aggregate unit_cost as the provisional.
- FIFO/LIFO pool with `provisional_basis = 'standard'`: depletion records standard_cost.unit_cost as the provisional.
- 'standard'-basis FIFO pool with no standard_cost row: verify RAISE EXCEPTION.
- Insufficient aggregate inventory under Path C: depletion exceeds aggregate qty → RAISE EXCEPTION → caller's tx aborts.
- Concurrent callers to hot FIFO pool via direct Path C: verify lock contention is minimal (lock-hold time per trx is constant w.r.t. pool's layer count). **This is one of two primary measurements Path C exists to validate.**

### 9.3 ledger-routed-c integration tests

- Submit FIFO depletion via ledger_enqueue_trx_c, poll for trx existence by (trx_type, source_id); verify trx + trx_line appear after committer processes.
- Concurrent submissions to overlapping FIFO pools: verify affinity grouping (one commit_group handles all overlap; single committer per group).
- Committer death mid-processing on a routed-c commit_group: verify orphan recovery picks up the commit_group; trx rows for submissions in the group either all exist (recovery committer's tx committed) or all don't (recovery committer reprocesses fresh).
- Failed submission within a routed-c commit_group: verify the failed submission is excluded, remaining submissions process successfully in the same tx WITHOUT pristine-replay (§14.2). The aggregate's qty/unit_cost end at the correct value given only the successful submissions.
- 'standard'-basis FIFO pool with 1000 concurrent submissions on one hot pool via ledger_enqueue_trx_c: verify the commit_group is processed in one PG tx with one pool_lock acquisition. **This is the second primary measurement Path C exists to validate — batching combined with provisional cost handling.**
- Postmaster crash with routed-c submissions in-flight: shmem lost, no trx rows from in-flight work, system accepts new submissions normally.
- Duplicate submission detection: same (trx_type, source_id) submitted twice via ledger_enqueue_trx_c. Pre-flight dedup (§6.4 step 5) catches it; no UNIQUE escapes.

### 9.4 Direct vs routed comparison tests

- Drive the same workload through ledger_submit_trx_c and ledger_enqueue_trx_c; verify final pool_state.aggregate qty matches. Provisional unit_cost values may differ because batch processing changes within-batch running-average evolution — that's expected and not a bug; recalc/close (deferred) would converge both to the same authoritative cost.
- Identify the concurrency threshold where routed-c overtakes direct-c on throughput: at low concurrency direct wins (no router overhead); at high concurrency on hot pools routed wins (batched aggregate update).

Recalc/close tests deferred per §7.

## 10. Workload configurations

The PoC's measurement harness drives Path C through controlled workloads, including three distinct submission modes for direct flavor plus the routed flavor — four total configurations comparable on the same workload axes.

### 10.0 Submission modes

- **Direct, per-call**: each `ledger_submit_trx_c` runs in its own user-tx (caller opens a tx, calls the SPI once, commits). The default direct-flavor pattern.
- **Direct, batched-per-caller**: caller opens one user-tx, calls `ledger_submit_trx_c` N times (configurable batch size; default 50), commits. Standard PG transactional batching per §5.5. No new SPI required.
- **Routed**: caller calls `ledger_enqueue_trx_c`, work is batched across callers by the router/committer machinery (§6).

(A fourth comparison point — direct per-call against a strict-mode implementation — requires running design-v3.md's Path A in parallel and is optional cross-document analysis.)

### 10.1 Caller concurrency

- Light: 1-4 concurrent caller sessions.
- Medium: 16-64.
- Heavy: 256-1024.

### 10.2 Pool overlap

- Disjoint: each session targets its own pool set.
- Light overlap: sessions overlap on 10-20% of pools.
- Heavy overlap: sessions overlap on most pools (Zipfian distribution).
- Pathological: 1000 sessions, all hitting one pool.

### 10.3 Trx complexity

- Simple: one trx_line, one pool.
- Medium: 3-5 trx_lines, multiple pools.
- Complex: 10-20 trx_lines, mixed receipts and depletions.

### 10.4 Method mix

- All FIFO (Path C's primary stress regime).
- All WAC (Path C has no advantage; baseline comparison).
- All STD (no provisional mode; baseline).
- Mixed: 50% FIFO, 30% WAC, 20% STD.

### 10.5 Pool depth (FIFO/LIFO-specific)

For FIFO/LIFO workloads, the existing layer count on a pool affects strict-mode performance:

- Shallow: ≤10 live layers per pool. Strict-mode layer iteration is fast.
- Medium: 10-100 live layers per pool. Strict-mode lock-hold time per depletion grows linearly.
- Deep: 1000+ live layers per pool. Strict-mode iteration dominates lock-hold time; **this is the regime where Path C's constant-time aggregate work matters most**. Path C demonstrates constant lock-hold time across depths; strict-mode comparison (showing linear growth) requires v3's Path A/B implementation, external to this PoC.

Depth is established by pre-seeding the pool with receipts before the measurement workload begins.

**Seeding mechanism.** Path C's hot path does NOT create layer rows for FIFO/LIFO pools (provisional mode updates only the aggregate). So the test harness cannot create deep pool state by driving Path C ingestion. The harness uses direct SQL — bulk `INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost) VALUES (...)` — to populate layer rows directly, simulating the state a strict-mode implementation would have produced. This is test-harness-only; production Path C deployments never need to create layer rows on the hot path (recalc/close would materialize them if FIFO/LIFO authoritative costs are required). The harness also writes corresponding trx_line receipt rows for each seeded layer so that the trx_line stream is consistent with the pool_state (allowing future recalc/close replay to verify against the seeded state).

### 10.6 Workload matrix

For the bake-off: cross-product of caller concurrency × overlap × complexity × method mix × pool depth. Representative scenarios:

- **S1**: Light concurrency, low overlap, simple trxs, all WAC, shallow pools. Baseline. Path C direct, Path C routed, and any strict-mode comparison path perform similarly (no contention, no layer iteration).
- **S2**: Heavy concurrency, high overlap, simple trxs, all WAC, shallow pools. Path C is identical to strict-mode WAC; tests routing's lock-contention amortization (routed-c should pull ahead of direct-c here).
- **S3**: Light concurrency, low overlap, complex trxs, mixed methods, shallow pools. Tests per-trx work intensity.
- **S4**: Heavy concurrency, Zipfian overlap, complex trxs, mixed methods, shallow pools. Stress-test, production-like.
- **S5**: Pathological — 1000 callers, all hitting one hot pool, simple FIFO trxs, shallow pools. Routed-c should dominate; direct-c serializes despite microsecond-fast lock-hold.
- **S6**: Pathological — 1000 callers, fully disjoint FIFO pools, simple trxs, shallow pools. Direct-c should win (no contention, lower overhead than router round-trip).
- **S7**: Heavy concurrency, Zipfian overlap, simple FIFO trxs, **deep pools**. **Path C's home field.** A strict-mode implementation in this regime would bottleneck on layer iteration (this is the architectural motivation for Path C; not directly measured in v3.1 since strict-mode implementation lives in v3). Direct Path C reduces lock-hold time per trx to aggregate-row work (each caller serializes through a microsecond-per-trx critical section, independent of pool depth). Routed Path C reduces serialization itself (1000 concurrent submissions become one commit_group with one aggregate update). Routed-c should overtake direct-c at high concurrency on overlapping pools.
- **S8**: Heavy concurrency, Zipfian overlap, complex FIFO trxs (multi-line), deep pools. Production-realistic FIFO stress. Same hot-path properties as S7 with more complex per-trx work.
- **S9**: S8's shape (1000 callers, Zipfian, complex deep-pool FIFO) with **multi-touch enabled** — a head-to-head against S8. ~40% of submissions touch one pool 2–3× (the WO-completion shape: backflush + scrap + output on one SKU/location), drawn from a weighted touch distribution (`1:60,2:30,3:10`) so the mix is realistic rather than a single synthetic worst case. Exercises `PlanResult::coalesce_aggregates` (§5.1, the keep-last collapse of same-pool aggregate mutations) under load, which S1–S8's distinct-pool generation never reaches.
- **S10–S21 (Pareto 80/20 family block, `acct-s90k`)**: extremes-bracketed (S1–S9) cover 0% (S6 disjoint) and 100% (S5 single-hot-pool) overlap and approximate the mid-market region via Zipf(1.2) (~87/20 over 10000 pools, more concentrated than textbook Pareto 80/20). S10–S21 add a *discrete* two-population mixture — each submission rolls hot vs cold by `hot_traffic_fraction` then samples uniformly within that population — × three workload families × four variants. **Families:** RECEIPTS (S10–S13, deplete 0, distinct-pool), BUILDS (S14–S17, deplete 50, multi-touch ON with the S9 preset), MIXED (S18–S21, deplete 50, distinct-pool). **Variants per family:** mid-typ (50 callers, 80/20), high-vol (200 callers, 80/20, depth 100), long-tail (50 callers, 90/10), balanced (50 callers, 50/50). Coverage extension only — not a new architectural premise.

**Multi-touch generation (acct-34ce).** By default a submission's lines land on DISTINCT pools, keeping per-submission pool count unconfounded for lock-hold measurement (§5.1). Two orthogonal knobs relax this: `multi_touch_pct` (fraction of submissions eligible to repeat a pool) gates eligibility, and a weighted per-pool **touch distribution** (`touches:weight,…`) shapes the repeats. The generator opens a fresh distinct pool per group (so repetition is orthogonal to which pools the overlap mode picks) and places that group's drawn touch-count of lines on it. Both are parameterizable per run via `run --multi-touch-pct` / `--touch-dist`, overlaying any scenario; S9 is the canned realistic preset. The default (`multi_touch_pct = 0`) path is byte-identical to distinct-pool generation.

**Pareto overlap (acct-s90k).** A new `OverlapMode::Pareto { hot_pool_fraction, hot_traffic_fraction }` sits alongside Uniform / Zipf / Disjoint as the fourth overlap mode. The picker rolls hot vs cold by `hot_traffic_fraction`, then samples uniformly in `pool_ids[0 .. hot_count]` (hot) or in the complement (cold), where `hot_count = round(hot_pool_fraction × n)` clamped to `[1, n-1]` whenever both populations should exist. CLI overlays `run --pareto-hot-pool-pct` / `--pareto-hot-traffic-pct` re-shape any base scenario; the S10–S21 presets bake the canonical 80/20 / 90/10 / 50/50 splits into named scenarios. Orthogonal to Complexity, `deplete_pct`, and multi-touch.

(Recalc/close-saturation scenarios are deferred along with recalc/close, §13.)

## 11. Success criteria

### 11.1 Correctness

Path C records what it claims to record: trx_lines with provisional unit_costs, NULL source_trx_line_id for FIFO/LIFO depletions, aggregate-only pool_state mutations for FIFO/LIFO. Verified by integration tests (§9).

Cross-flavor equivalence: direct-c and routed-c, given identical input sequences, produce identical pool_state.aggregate.qty values (in oversell scenarios this holds within a commit_group but not necessarily across the split chunks of a large component, whose interleave is not fixed — §14.2 split-chunk note, §15). For **receipts-only** pools they also produce identical unit_cost — value_sum accumulates exactly, so the running average is `banker_div(Σ Q×C, Σ Q)` regardless of processing order (§3.0/§3.1). For pools with **depletions**, unit_cost may still differ across flavors because of within-batch order-of-processing: depletion-at-rounded-average is lossy, so once a depletion intervenes the running average is order-sensitive (a deliberate consequence of routed flavor's batching, not a bug). Recalc/close (deferred) would converge both flavors to identical authoritative costs.

### 11.2 Direct flavor demonstration

Direct Path C under S7 (heavy concurrency, FIFO, deep pools) demonstrates that lock-hold time per trx is **constant w.r.t. pool depth**. The PoC measures this directly: pre-seed pools at depths of 10, 100, and 1000 layers (per §10.5 seeding mechanism); run identical workloads against each; measure per-trx lock-hold time; verify it does not grow with depth.

This is the architectural premise. If lock-hold time grows with pool depth, the premise has failed.

(Comparison to strict-mode lock-hold time — which DOES grow linearly with depth — requires a strict-mode implementation. design-v3.md's Paths A and B provide this baseline if both PoCs are run; v3.1 alone validates only the self-referential constant-time property.)

Hot-path throughput per caller session should be insensitive to pool depth as a direct consequence — a depletion against a 1000-layer pool takes the same lock-hold time as against a 5-layer pool.

### 11.3 Routed flavor demonstration

Routed Path C under S5 and S7 demonstrates throughput that exceeds direct Path C on hot pools — the batched commit_group reduces 1000 individual pool_lock acquisitions to one. This is the answer to "what happens when 1000 callers hit one deep FIFO pool simultaneously": neither direct Path C (1000 serialized microsecond turns) nor any strict-mode implementation gives a satisfying answer at very high concurrency. Routed Path C does.

### 11.4 Crossover identification

Identify the concurrency × overlap × method × depth × submission-mode region where each Path C configuration wins. Three submission modes are compared (§10.0):

- **Direct, per-call**: each `ledger_submit_trx_c` runs in its own user-tx. Lowest per-trx overhead at low concurrency on disjoint pools. Bottlenecks on per-trx fsync and per-trx pool_lock acquisition under heavy load.
- **Direct, batched-per-caller**: caller wraps N submissions in one user-tx. Amortizes fsync and within-tx lock-acquisition cost across N. Wins over per-call when callers naturally have multiple trxs to submit at once.
- **Routed**: cross-caller aggregation via shmem and committer pool. Wins over both direct modes when many independent callers concurrently target overlapping pools — neither direct mode aggregates across callers.

Expected wins by regime:

- **Direct, per-call wins**: light concurrency, disjoint pools, callers with one trx at a time.
- **Direct, batched-per-caller wins**: moderate-to-heavy concurrency where each caller has multiple trxs to submit. Captures most of routed's batching benefit at zero shmem/committer complexity.
- **Routed wins**: high concurrency, overlapping pools, callers that submit one trx at a time and can't batch on their own side. The cross-caller aggregation is what routed alone provides.

The key crossover the PoC characterizes: **does routed win meaningfully over direct-batched-per-caller, or does standard-tx batching capture most of routing's benefit?** If direct-batched matches routed across realistic workloads, the production answer is "tell callers to batch their submissions in a user-tx" rather than building the routed flavor. If routed pulls dramatically ahead on hot-pool workloads, the routing complexity is justified.

The PoC characterizes the crossover surface empirically.

### 11.5 Failure mode coverage

**Direct flavor** failure scenarios:
- Hot path failure (insufficient inventory, missing standard_cost): RAISE EXCEPTION, caller's tx aborts, no trx row created.
- Postmaster crash: in-flight hot-path trxs (uncommitted) aborted by PG; already-committed trxs durable.

**Routed flavor** failure scenarios (invariant: trx exists iff successfully recorded):
- Failed submission within a commit_group: excluded from the working snapshot, no trx row, remaining submissions in the group continue and produce their trx rows normally.
- Committer death: identity registry detects via dead pid; next committer reclaims the commit_group via CAS that swaps the (slot, token) on the queue entry; reclaiming committer reprocesses unconditionally and relies on §6.4 step 5's pre-flight dedup against trx (plus the trx UNIQUE constraint as backstop) to skip any submissions the dead committer already wrote.
- Router death: shmem boot sweep reverts in-flight staging entries to pending.
- Postmaster crash: shmem lost; in-flight submissions are lost; no DB cleanup needed because in-flight submissions never wrote trx rows.

In all cases: a trx row in the database represents a successfully recorded business event with a provisional cost. Its absence represents either "never submitted" or "submitted but didn't complete." Authoritative cost is established later by recalc/close.

## 12. PoC implementation plan

### Phase 1: schema + ledger-core

- Postgres schema (§2) installed: enums, tables, indexes.
- ledger-core crate: numeric.rs, snapshot.rs, plan.rs, error.rs, method.rs.
- Per-method implementations: wac.rs (strict), std.rs (strict), specific.rs (strict), fifo.rs (stub returning MethodMismatch), lifo.rs (stub returning MethodMismatch), provisional.rs (Path C dispatch — handles FIFO/LIFO aggregate updates; dispatches to wac.rs, std.rs, specific.rs for other methods).
- Comprehensive ledger-core unit tests (§9.1).

Deliverable: ledger-core compiles and unit tests pass.

### Phase 2: ledger-direct-c (Path C direct flavor)

- pgrx extension exposing ledger_submit_trx_c.
- Integrates ledger-core. Per-pool method dispatch (§5.1 step 6).
- Bulk write logic for aggregate-only updates (FIFO/LIFO provisional) and full layer mutations (specific strict).
- standard_cost lookup for STD pools and 'standard'-basis FIFO/LIFO pools.
- pool_lock acquisition.
- Error handling: caller-visible failure mode via RAISE EXCEPTION.

Deliverable: direct flavor operational. Submitting a FIFO depletion via ledger_submit_trx_c writes a trx_line with provisional unit_cost, updates only the aggregate row of pool_state, returns trx.id. Integration tests (§9.2) pass.

### Phase 3: ledger-routed-c (Path C routed flavor)

- Shmem layout: staging queue, committer queue, spillover arena, identity registry (§6.2).
- Router BGWorker: window scanning, union-find, affinity grouping, commit_group assembly (§6.3).
- Committer BGWorker pool: claim, pg_xact check, pre-flight dedup, hydrate, plan_apply_provisional dispatch, bulk write, commit (§6.4).
- **No pristine-snapshot replay** (per §14.2 — Path C has no cross-trx state dependency).
- Failed-submission handling: drop from working snapshot and continue.
- Recovery: router boot sweep, committer death handling via identity registry + pg_xact, postmaster restart (§6.5).

Deliverable: routed flavor operational. 1000 concurrent ledger_enqueue_trx_c submissions to one hot FIFO pool become one commit_group processed in one PG tx with one pool_lock acquisition and one aggregate UPDATE. Integration tests (§9.3) pass.

### Phase 4: ledger-harness

- Workload generator (separate binary, multi-session PG client).
- Configurable concurrency, overlap distribution, complexity, method mix, pool depth (§10).
- Pre-seeding harness for deep pools (direct SQL bulk insert into pool_state and trx_line per §10.5).
- **Three submission modes** (§10.0): direct per-call, direct batched-per-caller (configurable batch size), and routed.
- Drives all three modes through scenarios S1-S21 (S10-S21 = Pareto 80/20 receipts/builds/mixed family block, `acct-s90k`).
- Records throughput, per-trx lock-hold time as a function of pool depth, fsync rate, WAL volume, committed trxs/sec.
- Direct vs routed crossover measurement.

Deliverable: empirical throughput numbers for both flavors across the workload matrix; per-trx lock-hold time measurements at depths 10/100/1000 demonstrating constant-time behavior.

(Variance magnitude — the gap between provisional unit_cost and the strict-FIFO/LIFO-true unit_cost — is not measured in this PoC. Computing it offline requires running strict FIFO/LIFO replay against the trx_line stream, which means implementing fifo.rs/lifo.rs strict logic. That's the same code recalc/close needs; the natural place to add variance measurement is alongside the recalc/close implementation, not in this PoC. Business-risk assessment of provisional cost drift is a recalc/close-phase deliverable.)

### Phase 5: characterization

- Build crossover map: concurrency × overlap × method × depth → which flavor wins.
- Validate (or falsify) the hypothesis that routed Path C dominates the deep-FIFO + high-concurrency regime.
- Document findings.

Deliverable: PoC report. Specifically: the direct-vs-routed crossover threshold; the per-trx lock-hold time as a function of pool depth (should be constant for FIFO/LIFO under Path C); the throughput envelope for routed-c under hot-pool conditions. If v3's Path A/B implementations are also available, an additional cross-document comparison can show the depth at which Path C pulls ahead of strict-mode — but this comparison is optional for v3.1's PoC validation.

Recalc/close implementation is a separate future phase, outside this PoC's scope.

## 13. Out of scope

- **Recalc / close (authoritative FIFO/LIFO cost reconciliation).** Path C records provisional costs on the hot path; turning them into authoritative FIFO/LIFO costs by walking the trx_line stream, running layer math, and posting cost-adjustment trxs is deferred. Covers: worker/process model (continuous, on-demand, periodic batch); schema additions (cost_adjustment enum values, sequences, watermarks or settled-state on trx_line, layer-row materialization); concurrency between hot path and reconciliation; recovery semantics; backdated-receipt handling; audit linkage between depletions and the receipt layers that fed them. See §7.
- **Negative inventory.** Depletions that would drive aggregate qty below zero are rejected via InsufficientInventory. Production deployments needing negative inventory (transfer-shipment-before-receipt, backflushing-beyond-on-hand) would add a per-pool or per-trx-type allow-negative flag plus the WAC formula's negative-qty branch plus GL split-receipt accounting; see §3.6 for the full design surface.
- **Multi-currency.** Single currency assumed.
- **Effective-dated standard costs.** standard_cost has no temporal tracking. Backdated trxs use current standards. Production deployments add effective_from/effective_to columns.
- **account_balance denormalization.** Balances derived via SUM(amount) at query time.
- **Period close mechanics.** accounting_period table exists but close hooks are not implemented in PoC.
- **Webhook delivery.** Not in this PoC.
- **Multi-tenant isolation.** Single-tenant.
- **Identity-key extended dimensions** beyond what pool.identity_key carries.
- **Caller observability for routed flavor.** Polling for completion, knowing trx.id, error reporting back to the caller — out of scope for the PoC.

## 14. Concerns and open questions

### 14.1 Aggregate qty and unit_cost for FIFO/LIFO pools under Path C

The pool_state aggregate (layer_id = 0) for a FIFO/LIFO pool under Path C carries qty (the running net) and unit_cost (the running average maintained per WAC formula on every receipt). The aggregate qty is the authoritative on-hand count. The aggregate unit_cost is a Path C-specific construct — strict-mode implementations don't maintain a running average for FIFO/LIFO pools.

For pools with `provisional_basis = 'running_avg'`, this aggregate unit_cost IS the provisional cost basis used on depletions. For pools with `provisional_basis = 'standard'`, the aggregate unit_cost is still maintained (the WAC formula still runs on receipts) but is not used for depletions — depletions pull from standard_cost. The aggregate's running average remains useful for analytical queries even when not on the hot read path.

If a query reads `pool_state` for a FIFO pool and finds layer_id=0 with unit_cost=X, that X is the running average, not the "current FIFO cost" (which doesn't have a single meaningful value for a multi-layer pool). Queries needing layer-specific costs must wait for recalc/close or replay trx_line directly.

### 14.2 Pristine-replay is not used

> **Status (v3.1 PoC): SETTLED.** Implemented as drop-and-continue in
> `ledger-routed-c/src/committer.rs::plan_and_write` — a per-submission trial snapshot clone,
> discarded on `plan_apply_provisional` Err, no pristine-replay. Submission_id-ascending order
> holds *within* a commit_group; ordering *across* the split chunks of a large component is not
> guaranteed — chunks are independent commit_groups claimed concurrently by different committers
> with no predecessor-wait, so provisional unit_costs and oversell failure-sets may differ across
> orderings (`pool_lock` serializes same-pool writes but does not order them). See below.
> (AUDIT-PASS2.md §4.4 verified the within-group properties.)

Path B (strict routed) in some architectures uses pristine-snapshot replay to handle failures in a commit_group where one trx's plan_apply fails and excluded trxs would have left stale intermediate state visible to other trxs. Path C has no cross-trx state dependency on the hot path — each trx updates only the aggregate row and produces only its own trx_line/posting_line rows. A trx that fails plan_apply_provisional (e.g., depletion exceeds aggregate qty) is simply dropped from the working snapshot; remaining trxs proceed unchanged.

For direct flavor: the failed trx's user-tx aborts via RAISE EXCEPTION; nothing else is at stake.

For routed flavor (§6.4): the committer processes submissions in a single deterministic order — **submission_id ascending, which corresponds to enqueue order**. Within a single submission, lines are processed in the order the caller supplied them. The aggregate state evolves through the batch as submissions are processed. A failed submission (e.g., its depletion would drive aggregate qty below zero given the state of the working snapshot at the moment it's processed) is dropped from the snapshot; later submissions in the same commit_group see the snapshot without the failed submission's contribution.

**This is not commutative across submissions.** If submission A is "deplete 5 from pool X" and submission B is "receipt 10 to pool X", and they end up in the same commit_group:
- Enqueue order [A, B]: A processes first against the snapshot's starting qty. If starting qty < 5, A fails and is dropped. B then processes its receipt, updating the snapshot. Final state: starting + 10. A produced no trx row.
- Enqueue order [B, A]: B processes first, adding 10 to the snapshot. A then processes with qty = starting + 10, which is now ≥ 5. A succeeds. Final state: starting + 10 - 5. Both produce trx rows.

The two orderings produce different final states and different sets of recorded trxs. This is **intentional** — it preserves the strict-time-order semantics of the hot path. A depletion that arrives before its replenishing receipt fails, regardless of whether they end up in the same batch. Path C routed does not reorder submissions to make them succeed; doing so would change semantics in a way no strict-mode implementation would (under Path A, the equivalent caller-A's user-tx would have RAISED EXCEPTION at submission time).

Determinism: for a fixed processing order within a commit_group, the committer's behavior is deterministic — re-running the same commit_group's submissions in the same order produces identical outputs. Across the split chunks of a large component, committed by independent committers, the interleave is not fixed (§14.2 split-chunk note below), so repeat-run determinism of `pool_state.aggregate.qty` holds for workloads without cross-chunk oversell races — which covers every measured PoC scenario — but is not guaranteed in the general oversell case.

Pristine-replay (Path B's mechanism for restarting from a clean snapshot when an intermediate trx fails) is genuinely not needed here. Drop-and-continue suffices because there's no cross-trx state visible-but-uncommitted on the hot path that needs unwinding — each submission's contribution to the working snapshot is independent of every other submission's. The committer doesn't need to back out a failed submission's earlier writes (there were none to back out).

What IS true about commutativity: for **receipts only** (no depletions), the running average is **fully order-independent**. value_sum accumulates the exact book value (Σ Q×C) and unit_cost is derived as `banker_div(value_sum, Σ Q)`, so any permutation of the same multiset of receipts yields byte-identical final qty AND unit_cost (§3.0/§3.1) — this is the value_sum storage model's payoff over an incrementally-re-rounded average. Mixed batches (receipts + depletions) remain order-sensitive: a depletion subtracts the posted `Q × applied_unit_cost` (a rounded amount), so once a depletion intervenes the running average is path-dependent — and, when a depletion would oversell, drop-and-continue makes even qty order-sensitive (the A/B example above).

**Why there is no read-modify-write race on the running average.** The running average is never the target of two concurrent SQL read-modify-write cycles, so the determinism above does not rest on luck. Routing affinity (§6.3) places every submission touching a given pool into the *same* commit_group, claimed by exactly *one* committer, which folds that pool's receipts into the running average **in-memory** (working snapshot, submission_id order) and emits a single coalesced aggregate UPSERT (§6.7) — there is no per-receipt `UPDATE ... SET unit_cost = f(unit_cost)` round-trip to race on. When a connected component is split across commit_groups by `batch_size_max`, each committer holds `pool_lock FOR UPDATE` while it hydrates and writes, so whichever chunk acquires the lock second blocks until the first commits and then reads the post-commit aggregate (pool_lock serializes the two folds one at a time; it does not order them by queue position). In both cases the per-pool fold is serialized, and because value_sum is exact the receipts-only final average — `banker_div(Σ qtyᵢ·costᵢ, Σ qtyᵢ)` — is the same no matter how the receipts interleave. (The §9 property test asserts both the final-state qty determinism for any sequence, and, for receipts-only pools, that the final unit_cost equals this value-weighted average.)

**Split-chunk ordering across commit_groups.** When a connected component exceeds `batch_size_max` (§6.3), the router splits it into multiple commit_groups, preserving enqueue order in the *assignment* (chunk 1 gets the lowest submission_ids, chunk 2 the next, etc.). Ordering guarantees then differ by axis:

- **Within a chunk**, the committer processes submissions in submission_id order.
- **Across chunks**, order is *not* guaranteed. Each split chunk is an independent commit_group claimed by whichever committer is free (`committer_count` defaults to 4), with **no predecessor-wait** — a later chunk may be claimed and begin before an earlier chunk commits (`ledger-routed-c/src/router.rs`, "Path C delta vs a strict path", documents this deliberate drop). `pool_lock FOR UPDATE` serializes any two chunks that touch the same pool so their writes never interleave, but it does not order them by queue position.

The consequences track the commutativity split above. For **receipts-only** pools the final qty and unit_cost are order-independent (value_sum is exact; §3.1), so splitting changes nothing observable. For **mixed batches that oversell**, drop-and-continue makes the success/failure set order-sensitive (the A/B example above): which submissions fail — and hence the recorded provisional unit_costs and the final aggregate qty — may differ from what a single-chunk-with-no-cap committer would produce, and may differ between repeat runs. This is inside Path C's accepted semantics (provisional aggregate updates are allowed to differ across orderings, and a depletion that races ahead of its replenishing receipt is permitted to fail), but it is **not** the stronger "identical to single-chunk" guarantee. Every measured PoC scenario is free of cross-chunk oversell races, so the divergence is architectural, not observed.

### 14.3 Choice of provisional cost basis

> **Status (v3.1 PoC): SETTLED.** Both bases are implemented and dispatched in
> `ledger-core/src/provisional.rs` (`running_avg` reads the aggregate unit_cost; `standard`
> reads `standard_cost`). The two not-implemented bases (last-receipt, last-depletion) remain
> out of scope.

For FIFO/LIFO pools, the hot path needs to record SOME unit_cost for depletions, even though the true FIFO/LIFO cost is unknown at recording time. The PoC supports two bases, selectable per pool via `pool.provisional_basis`:

**`'running_avg'` (default).** Use the pool aggregate's running unit_cost, maintained per the WAC formula on every receipt. Self-contained — no external table lookup. Tracks recent receipts; generally close to true cost for slow-moving pools; can deviate when receipt costs are volatile.

**`'standard'`.** Use the standard_cost table's unit_cost for the pool's (sku_id, location_id). Predictable; the variance recalc/close has to correct is bounded by deliberate standard-vs-actual deltas. Requires standard_cost to be populated.

Two additional bases were considered and not implemented:
- Last receipt's unit_cost. Closer to LIFO-true for stable-cost pools, but stale for FIFO and doesn't smooth volatility.
- Last applied depletion cost. Strong recency bias on volatile pools.

Both implemented options produce identical lock-hold characteristics on the hot path: `running_avg` reads one column from pool_state, `standard` reads one column from standard_cost. Either way it's a constant-time read followed by an aggregate update. The basis choice affects only variance magnitude that recalc/close (deferred) would later correct.

**Basis × routed flavor interaction.** Under routed Path C, the basis choice has a subtle batching consequence:

- `'running_avg'`: the committer's working snapshot maintains the running average across batch members. Receipts within the batch update old_unit_cost; subsequent depletions in the same batch see the updated value. The recorded provisional cost for a depletion thus depends on the batch's internal processing order. Not wrong, just different from direct flavor — and the difference washes out at recalc/close.
- `'standard'`: the committer reads standard_cost once at hydration; every depletion in the batch uses the same value. No within-batch state evolution. Pure data shuffle. Simplest, fastest path through the committer.

For pools where the business already maintains standard costs, `'standard'`-basis routed Path C is the architecturally cleanest configuration — no order-dependency within or across batches, fully order-independent recording.

The basis can be changed by operators (UPDATE pool SET provisional_basis = ...) at any time. The change applies to future depletions; already-recorded trx_lines retain their provisional costs as recorded. Recalc/close would correct any inconsistency at reconciliation time.

### 14.4 Reverse operations

A reverse/cancel of a previously-recorded trx is a new business event: it would be recorded as a new trx with negative qty signaling the reversal. The hot path treats it like any other depletion or receipt, updating the aggregate per the §3.1 WAC formula.

Layer-level "marking" (Dynamics 365's feature where a reversal exactly releases the layers consumed by the original) requires recalc/close to interpret the source_trx_line_id linkage. The schema already supports the linkage (source_trx_line_id column on trx_line); the recalc/close logic to honor it is deferred.

### 14.5 trx_line ordering and id allocation

trx_line.id is an auto-allocated identity column (declared `GENERATED ALWAYS AS IDENTITY`; functionally equivalent to BIGSERIAL). Ordering within a pool uses `trx_line.id` ascending — the identity allocation is globally monotonic in allocation order.

Concerns:
- **id is globally monotonic, not per-pool dense.** A pool's trx_line ids will be sparse (e.g., 5, 17, 42, 109) because other pools take intervening values. Doesn't matter for ordering; only the relative order within a pool matters.
- **BIGINT exhaustion.** Signed 64-bit; max value ~9.2 × 10^18. Not a concern.
- **Backdated receipts under Path C.** A receipt with business-effective posted_at preceding earlier-allocated receipts in the same pool gets a HIGHER trx_line.id (because identity allocation is in INSERT order, not posted_at order). The hot path doesn't use trx_line.id for chronological ordering — it just appends. Recalc/close (deferred) decides whether to order by trx_line.id (allocation order) or by trx.posted_at (business-effective order) when reconciling.

### 14.6 Identity-column allocation vs commit order

PostgreSQL identity columns (and BIGSERIAL) allocate ids monotonically via the underlying sequence's nextval(), but transaction COMMIT order is not necessarily the same. If Tx1 allocates id=10 and Tx2 allocates id=11, Tx2 might commit first.

This does not affect Path C's hot path because the hot path doesn't observe any cross-trx ordering of trx_line ids. Each trx independently reads aggregate state, updates it, writes its own trx_line.

When recalc/close is built (deferred), whoever builds it must account for this — a watermark-based "advance past max visible id" scheme is broken under BIGSERIAL-without-commit-ordering. Standard fixes include settled-state columns, full-recompute idempotency, or txid_snapshot-based safe-watermark patterns. This is a recalc/close design decision.

## 15. Implementation divergences (v3.1 PoC)

Recorded by the post-build coherence review (`poc/ledger-v3.1/AUDIT.md`, `AUDIT-PASS2.md`). The
PoC faithfully realizes this spec; the deltas below are the known divergences. None is a
correctness defect. This section lists only **spec-relevant** divergences; per-finding detail
(file:line anchors, severities) and the code-side follow-ups the review shipped (crate extraction,
dead-scaffolding removal, the arena-leak fix, etc.) live in the AUDIT docs and
`poc/ledger-v3.1/README.md`.

- **SPI line shape** (§4): `lines` is a JSONB array-of-objects, not a SQL composite ARRAY; each
  object carries `(line_type, source_id?, pool_id, qty, unit_cost)`. Posting accounts (debit /
  credit, and the variance account for §3.3 STD receipts) are NOT on the wire — they are resolved
  ledger-side from `posting_account_map` per §3.7. Annotated inline at §4.
- **Aggregate-mutation coalescing** (§5.1 step 8 / §8): `ledger-core`'s
  `PlanResult::coalesce_aggregates` collapses per-pool aggregate upserts to one (keep-last) so a
  single submission touching a pool twice writes one `(pool_id, layer_id=0)` row (the direct
  `ON CONFLICT DO UPDATE` batch cannot touch a row twice). The routed committer reaches the same
  one-aggregate-per-pool shape via the §6.7 post-pass-snapshot reconstruction.
- **Harness, distinct pools per submission** (§10.3, §10.6): the workload generator emits distinct
  pool_ids per submission by default for clean lock-hold measurement (same root cause as the coalesce
  above). The coalesce path's correctness is covered by ledger-core / direct / routed tests; an
  **opt-in multi-touch mode** (acct-34ce — `run --multi-touch-pct` / `--touch-dist`, and the canned
  S9 preset) additionally drives same-pool-twice submissions through both flavors under load. Default
  stays distinct-pool.
- **Harness seeds `standard_cost`** (§10.4): the pool seeder must populate `standard_cost` for
  every std-method / standard-basis pool, else those pools abort with MissingStandardCost and
  confound mixed-method scenarios.
- **`qty >= 0` and `value_sum >= 0` CHECKs on `pool_state`** (§2.2/§3.6): no-negative-inventory began
  as a `ledger-core`-only code invariant; migration `0006` added `CHECK (layer_id <> 0 OR qty >= 0)`
  (`pool_state_aggregate_qty_nonneg`) and migration `0007` added the sibling `CHECK (layer_id <> 0 OR
  value_sum >= 0)` (`pool_state_aggregate_value_sum_nonneg`) — both schema-level defense-in-depth
  backstops on the aggregate row (scoped to `layer_id = 0` so they never constrain strict-method
  layer rows, which Path C does not materialize on the hot path). The `value_sum` column the second
  CHECK guards is a core storage column (migration `0007`, `NOT NULL`) carried in the §2.2 DDL.
- **Cross-chunk ordering is not guaranteed** (§14.2/§11.1): when a connected component is split
  across commit_groups by `batch_size_max`, the split chunks are independent commit_groups claimed
  concurrently by different committers with no predecessor-wait (`ledger-routed-c/src/router.rs`,
  "Path C delta vs a strict path"). `pool_lock FOR UPDATE` serializes same-pool writes but does not
  order them by queue position. Consequence: for receipts-only pools nothing observable changes
  (value_sum is order-exact); for mixed batches that oversell, the drop-and-continue success/failure
  set — and hence recorded provisional unit_costs and final aggregate qty — may differ across
  orderings and between repeat runs. This is within Path C's accepted semantics (provisional updates
  may differ across orderings) and is unobserved in all measured scenarios; the strict "identical to
  single-chunk" reading of the §11.1 qty-equality criterion holds within a commit_group only.
- **Router defaults tuned by acct-p1al** (§6.3): the production defaults are `batch_size_max` = 200
  and `router_pack_disjoint` = ON, set by the acct-p1al batch-formation sweep (measured
  win-or-neutral: spread 2×, deep-zipf +170%, mixed neutral, single-hot-pool inert). The
  disjoint-packing pass (acct-xdwk lever 1b) first-fit-bin-packs disjoint affinity components up to
  `batch_size_max` to amortize per-group commit/fsync on spread/Pareto workloads; off preserves
  one-commit_group-per-component. Separately, `batch_window_us` (default 500μs) is an
  oldest-candidate coalesce *dwell* evaluated once per tick, not the scan cadence — the router scans
  on a 50ms `wait_latch` loop.
