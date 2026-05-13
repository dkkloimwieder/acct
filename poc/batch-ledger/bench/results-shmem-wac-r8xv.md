# acct-r8xv — `ledger_apply_batch` minimal variant: falsified

The minimal r8xv variant replaced `post_batch_wac_shmem`'s per-leg
`PERFORM ledger_apply_balance_delta` loop (mig 0014) with a single
`PERFORM ledger_apply_batch(jsonb)` call (mig 0016) — collapsing
2N plpgsql → Rust cross-boundary calls per batch into one.

The hypothesis: cross-boundary call overhead (estimated ~10-50 µs ×
2000 per batch = 20-100 ms) was a meaningful fraction of the fan-out
batch cost (88 ms median at B4).

**Verdict: falsified.** Net throughput is noise-band-equivalent on
fan-out, slightly worse on fan-in.

## Measurements

3 replicates × 60s × 20 workers × batch=1000, same methodology as B4
(`bench/results-shmem-wac.md`).

### Per-run throughput (tps)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in mutable        | 23,226 | 24,948 | 24,694 | **24,948** |
| fan-in shmem (mig 0014, per-leg)    | 53,951 | 56,136 | 56,908 | **56,136** |
| fan-in shmem (mig 0016, batch-apply) | 50,861 | 49,339 | 52,842 | **50,861** |
| fan-out mutable       |  4,063 |  4,387 |  4,341 |  **4,341** |
| fan-out shmem (mig 0014, per-leg)   | 11,271 | 12,001 | 11,335 | **11,335** |
| fan-out shmem (mig 0016, batch-apply)| 11,383 | 12,325 | 12,122 | **12,122** |

### Headline deltas (mig 0014 → mig 0016)

| Shape | per-leg tps | batch-apply tps | Δ |
|---|---|---|---|
| fan-in  | 56,136 | 50,861 | **-9.4%** |
| fan-out | 11,335 | 12,122 | **+6.9%** |

Both within rig-noise-band per the B4-prep / ezm methodology (IQR
~15-20%); the fan-in regression is at the edge.

## Why minimal r8xv didn't help

The call-overhead estimate was wrong. Cost factors per batch=1000
that are NOT eliminated by collapsing the per-leg calls:

1. **plpgsql FOR LOOP control flow** — iterating over 1000 staging
   rows in `_wac_shmem_batch_staging` regardless.
2. **`jsonb_set` running-avg map** — O(N) per envelope, quadratic
   across the batch as the map grows.
3. **Per-envelope INSERT INTO `_wac_shmem_batch_staging`** — each
   row is a separate INSERT.
4. **NEW in mig 0016: `jsonb_agg` + `UNION ALL`** to build the legs
   array adds back roughly what the call coalescing saved.
5. **NEW in mig 0016: serde_json deserialization** in the extension
   to walk the JSONB and re-pack per-leg structs.

The actual cross-boundary cost was apparently small enough that
collapsing it produced a net wash. The plpgsql per-envelope work
(items 1-3 above) is the dominant cost — and mig 0016 preserved it
identically.

## What this means for the gap to acct-togd's 7-10× fan-out projection

The remaining gap is plpgsql work, not call overhead. Closing it
requires moving the WAC running-avg dispatch into Rust:

- Today's plpgsql: builds `v_pool_value` / `v_pool_qty` JSONB maps
  per pool, walks envelopes in order, updates the maps via
  `jsonb_set`, computes per-envelope amount.
- Maximal r8xv would: pass the raw envelope JSONB into Rust; Rust
  reads pool state via the shmem hash directly (no SQL round-trip);
  maintains the running-avg map as a `HashMap<u128, (i64, i64)>` in
  the function-local scope; computes amounts; stages into
  PENDING_STACK.

That's a structural refactor. The plpgsql `post_batch_wac_shmem`
becomes a thin wrapper over a Rust entry point that does:
- INSERT posting_lines (still SQL, via a CTE the Rust fn invokes
  via SPI, OR returned to the caller as a JSONB of posting-line
  records to be inserted)
- Apply staged deltas via PENDING_STACK (existing A2 path)

Effort estimate: 3-5 days. Tests pin correctness; bench should land
the projection's 7-10× lift if the plpgsql-dominance hypothesis is
correct. Filed as `acct-r8xv-maximal` if pursued; not in this push.

## Decision

mig 0017 reverts `post_batch_wac_shmem` to mig 0014's per-leg PERFORM
body (no perf change, simpler code path). The `ledger_apply_batch`
extension fn (lib.rs r8xv addition) stays installed as a primitive —
the maximal variant can build on it, or a maximal variant might
bypass it entirely depending on shape.

mig 0016's body is preserved in git history; if a future caller has
a workload where call overhead IS dominant, it remains a reference
implementation.

## Falsification value

The negative result is more useful than a +5% confirm would have
been: the WAC fan-out ceiling is now confidently attributed to
plpgsql per-envelope work, NOT call overhead. Any future
optimization that doesn't address the plpgsql FOR LOOP is wasted
effort.

## Raw logs

`/tmp/poc-r8xv-bench/{fanin,fanout}_wac_{mutable,shmem}/run_{1,2,3}.log`,
`/tmp/poc-r8xv-bench/summary.txt`. Bench-host only.
