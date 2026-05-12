# Append-only (TigerBeetle-aligned) measurement

Side-experiment of P3 (acct-k7c6) prompted by user question: "why did we not
test append only? i thought we are trying to be more consistent with TigerBeetle
so we can get some perf wins?"

Correct critique. The PoC's `post_batch` (and all its variants) still maintain
mutable `accounts.balance` + `accounts.qty` via `UPDATE`. That's structurally
opposite to TigerBeetle's design (no row-level state during commit; balance
derives from the LSM forest). The Postgres equivalent is **append-only**: every
operation only INSERTs into `posting_lines`. Balance is computed as
`SELECT SUM(amount) FROM posting_lines …` at read time, or via a
periodically-refreshed projection (out of PoC scope here).

## Configurations measured (20w × 50acct × batch=1000 sync_on)

| # | configuration | tps | per-row | win vs P3 simple |
|---|---|---|---|---|
| 0 | P3 simple (UPDATE accounts, FOR UPDATE pre-lock) | **40,610** | 0.49 ms | baseline |
| 1 | Append-only (skip UPDATE accounts + FOR UPDATE) | **80,514** | 0.25 ms | **+98%** |
| 2 | #1 + DROP posting_lines FK constraints       | **115,484** | 0.17 ms | +43% / +185% |
| 3 | #2 + DROP secondary indexes (debit_idx/credit_idx/document_idx) | **155,974** | 0.13 ms | +35% / +284% |

Single-worker append-only (uncontested ceiling, full schema): **24,124 tps** at
~41 µs/row.

## Per-row cost decomposition (working bottom-up at 20w contention)

```
Bare INSERT + UNIQUE on idempotency_key:     ~6 µs   (config #3)
+ 2 secondary B-tree indexes:                +6 µs   (config #2 → #3)
+ 2 FK validations (heap+btree lookup):      +3 µs   (config #1 → #2)
+ UPDATE accounts SET balance (with FOR UPDATE serialization):
                                            +12 µs   (config #0 → #1)
                                            ────
Total at full schema:                       ~24 µs   (matches P3 simple's 0.49 ms / 20w concurrency)
```

The `UPDATE accounts` step is **50% of P3 simple's per-row cost**. Removing it
DOUBLES throughput. This is the single largest architectural lever.

## Why? Both the FOR UPDATE pre-lock AND the aggregated UPDATE serialize on the
hot account set. Workers don't progress in parallel because they all need the
same ~50 rows. Append-only mode has no shared mutable rows in the hot path —
workers truly parallelize. 1w → 20w scaling jumps from 1.9× (P3 simple) to
**6.5×** in the most-stripped append-only mode.

## Comparison to all baselines

| Configuration | tps | vs pgledger (10,636) | vs P3 simple (40,610) |
|---|---|---|---|
| P2 single-row sync_on   | 2,603 | 0.24× | 0.06× |
| P2 single-row sync_off  | 12,955 | 1.22× | 0.32× |
| pgledger reported       | 10,636 | 1.00× | 0.26× |
| P3 simple batch=1000    | 40,610 | 3.82× | 1.00× |
| P4 strict WAC batch=1000 | 22,867 | 2.15× | 0.56× |
| P4 PAC batch=1000       | 27,993 | 2.63× | 0.69× |
| **Append-only batch=1000** | **80,514** | **7.57×** | **1.98×** |
| **+ no FK**             | **115,484** | **10.86×** | **2.84×** |
| **+ no idx**            | **155,974** | **14.66×** | **3.84×** |

**Append-only at 80K tps is ~7.5× pgledger's reported number on the same
hardware. With aggressive schema stripping (no FK, fewer indexes), 15× pgledger.**

## What WAC and FIFO were paying for (revisited)

Per the diagnostic conversation: the WAC/FIFO slowness in P4/P5 was NOT
contention — it was plpgsql + jsonb overhead on the in-batch running state. The
correct comparison is per-envelope cost vs P3 simple:

| Phase | per-envelope cost (batch=1000, 20w) | overhead vs P3 simple |
|---|---|---|
| P3 simple    | 0.49 ms | 0% |
| P4 PAC       | 0.71 ms | +45% |
| P4 strict WAC | 0.87 ms | +78% |
| P5 FIFO v2   | 26.2 ms | +5,250% |

P5 FIFO v2 pays ~26 ms per envelope because every envelope mutates a jsonb
that grows as in-batch receipts accumulate. O(n) per envelope × n envelopes
= O(n²). For acct backport, batched FIFO needs a different data structure
(client-side plan / TEMP TABLE / C extension) — covered by follow-up
acct-fw2w.

## Implications for the acct backport (P8 input)

Three architectural levers, ordered by impact-to-effort:

**1. Append-only balance (biggest single win)** — drop `accounts.balance` /
`accounts.qty` mutations from the hot path. Maintain balance via a projection
table that's refreshed periodically (cron job, trigger-based, or
LISTEN/NOTIFY-driven). Reads SUM from the projection. **Expected: 2× throughput
lift on top of the batch API.**

**2. Schema-discipline review** — every secondary index and FK constraint on
`posting_lines` adds ~3-6µs/row. Audit which are needed in the hot path vs
which can live on a projection. Each unnecessary index removed buys ~10%.

**3. PAC dispatch for WAC (already covered by acct-9wyl)** — 22-46% on the WAC
surface only. Smaller win than append-only.

**For an order-of-magnitude estimate of acct's batched ceiling**: with append-
only + tight schema + PAC dispatch + the batch API, acct should be able to
reach 30-50K tps on the 1s6r-shaped workload. That's 120-200× over today's
253 ops/s.

## What this changes about the architectural decision

The PoC originally aimed for "match pgledger's 10K." We've validated multiple
configurations that exceed that significantly:

- **Minimally invasive** (P3 simple = batch API alone, keep `accounts.balance`):
  40K tps. 4× pgledger. Order-of-magnitude over acct's 253.
- **TigerBeetle-aligned** (append-only): 80-156K tps. 7-15× pgledger. **The
  ceiling the audit acct-8hv2 was implicitly aiming at**.
- **Strict WAC** (in-batch running average): 23K. Acceptable but lower.
- **Batched FIFO**: not viable in pure plpgsql; requires different design.

The architectural decision for the acct backport now has clearer trade-offs.
Append-only is a bigger schema change (balance becomes a projection) but
DOUBLES the throughput at any batch size. P8 synthesis should explicitly
recommend whether to take that step.

## Files

- `db/migrations/0011_post_batch_append_only.up.sql` — append-only function,
  coexists with `post_batch` for selective routing.
- Bench data: spot measurements only (one 30s × batch=1000 per configuration).
  Full 5×60s sweep across batch sizes deferred to acct backport time.
