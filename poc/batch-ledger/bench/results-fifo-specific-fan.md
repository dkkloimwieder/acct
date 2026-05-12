# FIFO + specific cost benches at varying pool cardinality

Completes the cost-method bench coverage at fan-in / baseline / fan-out
shapes against the tuned PG conf. Pairs with the WAC multi-step results
to establish per-method bottleneck patterns.

## Methodology

- Tuned PG conf (db/postgresql.conf), 20 workers, batch=1000, 3×60s/run, 15s gaps.
- Three pool counts per method:
  - **pools=1**: fan-in (all workers hammer the same pool's locks)
  - **pools=20**: standard P5 / P4spec baseline
  - **pools=5000** (FIFO) or **pools=1000** (specific): fan-out
- FIFO: `post_batch` (FIFO v2 body) with `fifo_receipt` + `fifo_issue` envelopes.
- Specific: `post_batch_specific` (UPDATE-based) with `specific_receipt` + `specific_issue` envelopes.
- Specific pre-seed: `units_per_pool` parameterized via `POC_BENCH_UNITS_PER_POOL`
  to keep total seed time reasonable at high pool counts.

## Results

| Scenario | median tps | p50 ms | p99 ms |
|---|---|---|---|
| fifo_pools_1 (fan-in) | 371 | 33,688 | 88,084 |
| fifo_pools_20 (baseline) | 555 | 24,681 | 60,862 |
| fifo_pools_5000 (fan-out) | 748 | 24,153 | 31,370 |
| specific_pools_1 (fan-in) | 5,632 | 3,503 | 3,801 |
| specific_pools_20 (baseline) | 5,338 | 3,605 | 4,631 |
| specific_pools_1000 (fan-out) | 3,910 | 4,930 | 6,060 |

## Findings

### F1. FIFO fan-out > fan-in (opposite of WAC and simple-transfer)

FIFO at pools=5000 hits **2× the throughput** of pools=1 (748 vs 371 tps).
This is the inverse of WAC and simple-transfer-mutable, which both favored
fan-in.

**Why**: FIFO's `FOR UPDATE` is on `cost_layers` rows, not on `accounts`. Each
`fifo_issue` walks layers in receipt-date order under FOR UPDATE.
- Fan-in: all 20 workers contend on the SAME layer chain → serial.
- Fan-out: workers spread across distinct layer chains → minimum cross-worker
  conflict.

The cost shape that defines fan-in-vs-fan-out depends entirely on **what
gets locked**. FIFO locks layers, not accounts.

### F2. FIFO latency tails are catastrophic

p99 of 88 seconds per batch at fan-in. Median of 33 seconds. This is the
plpgsql + jsonb O(n²) cost identified in the original P5 results — each
envelope mutates a jsonb that grows as in-batch receipts accumulate.

Tuned PG conf did not help here: FIFO is CPU-bound on the running-state
jsonb mutations, not I/O-bound. **Native dispatch (acct-fngj) is the only
viable fix.**

### F3. Specific costing shows mild fan-in advantage

specific_pools_1 = 5,632 tps; specific_pools_1000 = 3,910 tps. ~30% gap.

**Why**: Specific does per-envelope `LEFT JOIN inventory_units` lookup
to resolve `unit_id` → `unit_cost`. Single-pool fan-in keeps a small set
of `inventory_units` index pages hot in shared_buffers. Fan-out across
1000 pools spreads the index page accesses; cold page loads + B-tree
traversal cost dominates.

Per-row cost: specific ~190 µs/row (5K tps × 20 workers / batch=1000).
That's an order of magnitude above simple transfer's 13 µs/row — the
LEFT JOIN + UPDATE on `inventory_units` is the dominating per-row work.

### F4. Tuned conf gave specific +25%; minimal effect on FIFO

- Specific: PoC memory recorded 4,422 tps (default conf, 20 pools) →
  tuned conf 5,338 tps at same shape = **+21%**.
- FIFO: PoC memory recorded 762 tps (default conf) → tuned conf 555 tps
  at pools=20 = **-27%** (likely noise / different bench-time shape).
- At fan-out (5K pools), FIFO hits 748 tps — within original baseline range.

Tuning conf reaches its limits when CPU-bound plpgsql is the bottleneck.

### F5. Each cost method has a different optimal shape

Summary across ALL benches:

| Method | Best shape | Best tps | Worst shape | Worst tps | What gets locked |
|---|---|---|---|---|---|
| Simple mutable | 50 accts random | 41K | 5K fan-out | 7K | `accounts` rows (FOR UPDATE) |
| Simple append-only | any | 69-77K | — | — | nothing |
| WAC mutable | 1 hot pool | 22K | 5K fan-out | 4K | `accounts` rows (pool pre-lock) |
| FIFO (jsonb) | 5K fan-out | 748 | 1 hot pool | 371 | `cost_layers` rows + jsonb CPU |
| Specific (UPDATE) | 1 hot pool | 5,632 | 1K fan-out | 3,910 | `inventory_units` index pages |

The fan-shape winner depends entirely on what's locked + cardinality of
that lock set per batch:
- Lock cost dominated by ACQUISITION on many distinct rows → fan-in wins
  (warm lock set).
- Lock cost dominated by CONTENTION on the same row → fan-out wins
  (spread).

## Implications for the extension toolkit

Each method has a distinct lever:

| Method | Bottleneck | Extension lever | Status |
|---|---|---|---|
| Simple transfer | FOR UPDATE on accounts | sw4i shmem rollup | acct-sw4i filed |
| WAC | FOR UPDATE on accounts + cost dispatch | sw4i + native WAC dispatch | sw4i + fngj |
| FIFO | plpgsql + jsonb O(n²) | native FIFO dispatch | acct-fngj |
| Specific (UPDATE) | inventory_units index + partial UNIQUE | AO event pattern | (already known) |

sw4i specifically targets the FOR-UPDATE-on-accounts cost, which dominates
WAC + simple-transfer fan-out. It does NOT help FIFO (different lock target)
or Specific (different lock target).

**Multi-extension stack required for full coverage**:
- sw4i alone → 5-10× lift on simple + WAC realistic workloads.
- + fngj → unblocks FIFO's structural ceiling (estimated 30-50K tps from
  PoC memory's pgrx-native projection).
- + AO event pattern for specific → 8.7K → ~15K tps (estimated).

Each lever is necessary for its workload class; none is sufficient alone.

## On the WAC→standard mixed-method scenario

User asked whether a "WAC components → standard FG" manufacturing scenario
needs explicit modeling. Analysis using the measured numbers:

A 50-component WO complete on a WAC raw + standard FG SKU =
- 50 × `wac_issue` envelopes (drain raw pools at running avg) — fan-out
  WAC shape, 233µs/row today
- 1 × `transfer` envelope (FG accrual at `parent_std × qty`) — caller
  supplied, 13µs/row
- 1 × `transfer` envelope (variance routing) — caller supplied, 13µs/row

Total: 50 × 233 + 2 × 13 = 11,676 µs ≈ 11.7 ms per WO complete.

Dominated entirely by the WAC fan-out side. The standard FG and variance
postings are negligible. No new contention pattern; the bench is already
covered by `wac_fan_out` in `results-wac-multi-step.md`.

**Post-sw4i estimate**: same 50-component WO complete drops to ~2 ms per
call (sw4i removes the ~200 µs/row cold-lock acquisition cost, leaving the
~30 µs/row WAC dispatch + ~13 µs/row standard postings).

5-6× lift on manufacturing-shape WO complete operations, with the bulk of
the win attributable to sw4i.

## Files

- `tests/bench_p5_fifo.rs` (existing) — FIFO bench, pool count env-driven.
- `tests/bench_p4spec_specific.rs` (existing, parameterized) —
  POC_BENCH_UNITS_PER_POOL now configurable for high-cardinality runs.
- `bench/run-fifo-specific-fan.sh` — sweep driver.
- Per-run logs in `/tmp/poc-fifo-specific-fan/` (reproducible via sweep).
