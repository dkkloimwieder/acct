# Specific/actual costing measurement + FIFO improvement options

## Specific/actual costing

Side-experiment of P5 (acct-1hps) prompted by user question: "what about
exact/actual costing on inventory?"

Specific costing tracks each unit individually with its own recorded cost.
Used for serialized items, lot+serial tracked goods, high-value items. acct's
inventory_units model (mig 0061-0066, sxl2 epic) is the analogous schema.
**The PoC's `specific` variant tracks one unit per envelope; issues consume
a specified `unit_id` and price at that unit's recorded cost.**

### Designs measured

| Variant | Pattern | Hot-path writes per envelope |
|---|---|---|
| **naive** (`post_batch_specific`) | INSERT posting_line + UPDATE `inventory_units SET status='consumed'` | 1 INSERT, 1 UPDATE (with 2 index updates) |
| **append-only** (`post_batch_specific_ao`) | INSERT posting_line + INSERT into `inventory_unit_events` | 2 INSERTs (no UPDATE) |

### Results (batch=1000, sync_on, 1 vs 20 workers)

| Variant | 1w tps | 1w per-env | 20w tps | 20w per-env |
|---|---|---|---|---|
| P3 simple (no inventory) | 21,576 | 44µs | 40,610 | 0.49ms |
| Append-only batch (no inventory) | 24,124 | 41µs | 80,514 | 0.25ms |
| **Specific naive (UPDATE units)** | **3,944** | **232µs** | **4,422** | 4.5ms |
| **Specific append-only (event)** | **5,331** | **161µs** | **8,718** | 2.3ms |

### Findings

**F1. Specific costing is structurally more expensive than simple
double-entry.** Per envelope writes 2 rows (posting_line + inventory event)
instead of 1. The cost is real — you're maintaining per-unit identity.

**F2. UPDATE-based status flip is ~50% slower than INSERT-based event.**
The UPDATE inventory_units in the naive variant pays for heap row mod +
PK index update + partial-index page modification. The partial index pages
are hot across workers (shared B-tree pages per pool), so contention is
worse than row-level. Append-only design (INSERT event row) sidesteps this
entirely.

**F3. 20-worker scaling is ~2.2× over 1-worker for append-only specific.**
Better than naive's 1.1× (no scaling) but still well below the append-only-
batch's 3.3× scaling. The shared `inventory_units` PK index page contention
(during the LEFT JOIN lookup for pricing) is the residual bottleneck.

**F4. Specific costing at 8.7K tps batched is ~34× over acct's current
253 ops/s baseline.** Viable for lot+serial workloads despite per-envelope
cost being 5× P3 simple. The batch API still delivers a major lift.

**F5. Specific is structurally simpler to batch than FIFO.** No in-batch
layer state. Each envelope's cost is fully determined by the caller-provided
`unit_id`. Pure-SQL CTE chain works; no plpgsql FOR LOOP needed.

### Implications for acct backport

- The `inventory_units` E3-style design (acct-0kz / sxl2) batches well in
  the append-only event pattern.
- Replace status mutation on `inventory_units` with `inventory_unit_events`
  (acct already has this table). Reads derive status via the latest event
  per unit, or via a periodic projection refresh.
- Specific costing wrappers (post_so_ship for serialized SKUs, etc.) can be
  in the same batch as transfer/WAC/PAC envelopes — they pay their own per-
  envelope cost but don't block others.

## FIFO improvement options (analysis only; no implementation here)

Five paths, ordered by expected throughput and structural cleanliness:

### 1. Client-side planning (Rust-driven) — **recommended**

- Rust loads `cost_layers` snapshot for touched pools (one SELECT with FOR
  UPDATE on the pool accounts).
- Rust walks layers in memory, allocates slices per envelope, builds a
  structured plan: `Vec<(posting_line_data, Vec<(layer_id, qty_take)>)>`.
- Sends plan to SQL: one multi-row INSERT into posting_lines, one multi-row
  INSERT into cost_layer_depletions, one multi-row UPDATE on cost_layers
  via UPDATE-FROM-VALUES.
- Eliminates plpgsql + jsonb cost entirely.
- **Estimate: ~30-50K tps (close to P3 simple).**
- Trade-off: plan-building complexity moves into Rust. The schema stays
  simple. Optimistic concurrency or FOR UPDATE on accounts handles
  cross-batch correctness.

### 2. TEMP TABLE for layer state — middle ground

- `CREATE TEMP TABLE _layers ON COMMIT DROP` at batch start.
- Seed from `cost_layers` via INSERT-SELECT.
- Per envelope (plpgsql FOR LOOP): SELECT FROM _layers ORDER BY receipt_date
  LIMIT N WHERE qty > 0; UPDATE _layers SET qty -= take.
- Aggregate at end: multi-row INSERT into cost_layers + cost_layer_depletions.
- Avoids the O(n²) jsonb manipulation but still pays plpgsql FOR LOOP cost.
- **Estimate: 5-15K tps.**

### 3. Pure-SQL with window functions — theoretically fastest, hardest

- Window functions to compute cumulative pool consumption up to each envelope.
- LATERAL JOIN or gap-and-island patterns to match issues to specific layers.
- All in one big SELECT/CTE chain. No plpgsql.
- **Estimate: 30-40K tps.**
- Implementation complexity is the blocker.

### 4. Keep FIFO per-document on acct — zero-risk

- acct-cbss's three-walks-same-txn already works per-document.
- Batch only standard + WAC + PAC + specific; FIFO stays per-document.
- Cost: FIFO wrappers don't get the 50-150× batched lift.
- Acceptable if FIFO is a small fraction of throughput.

### 5. C extension — out of scope

### Recommendation

**Option 1 (client-side planning)** is the right architecture for the acct
backport. The Rust crate has direct sqlx access; moving the plan into Rust
is a clean architectural split (SQL = bulk operations, Rust = layer
arithmetic). File as the resolution path of acct-fw2w.

## Files

- `db/migrations/0012_post_batch_specific.up.sql` (TODO: bake this fix-up
  into a permanent migration; currently applied via psql for measurement).
- `tests/bench_p4spec_specific.rs` — bench harness.
- This document captures spot-measurement results; full 5×60s sweep
  deferred to the acct backport stage.
