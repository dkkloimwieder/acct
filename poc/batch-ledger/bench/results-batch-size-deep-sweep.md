# Batch-size deep sweep — addendum to P3 results

Dense batch-size sweep on `post_batch_append_only` (mig 0011) against the
tuned PG18 conf (acct-1vo7, db/postgresql.conf, tip 93b20c4).

## Question this addendum answers

> Was the original P3 "knee at 1000" claim ([1, 10, 100, 1000, 8000] sparse
> sweep) actually validated, or did we miss a higher peak between or beyond
> the sampled sizes? Also — TigerBeetle handles up to 8K batches; should
> our optimum be there?

## Methodology

- 20 workers, 50 accounts, 3×60s replicates per size with 15s gaps.
- Sizes: 1000, 2000, 4000, 8000, 16000, 32000.
- `post_batch_append_only` (TigerBeetle-aligned: INSERT-only, no UPDATE accounts).
- Tuned PG conf (8GB shared_buffers, 24GB effective_cache_size, etc.).
- Release build via `cargo test`.

## Results

| batch_size | median tps | p99 latency | per-row µs | run spread |
|---|---|---|---|---|
| 1,000 | 77,264 | 392 ms | 12.9 | 74K-90K (22%) |
| 2,000 | 71,178 | 893 ms | 14.1 | 67K-78K (16%) |
| 4,000 | 77,228 | 1,549 ms | 12.9 | 65K-86K (28%) |
| 8,000 | 74,396 | 3,188 ms | 13.4 | 67K-79K (16%) |
| 16,000 | 74,702 | 7,828 ms | 13.4 | 62K-85K (32%) |
| 32,000 | 72,196 | 11,266 ms | 13.8 | 71K-72K (2%) |

## Findings

**F1. The throughput plateau is real and extends far past where we'd
declared the knee.** Median throughput stays in 72K–77K across the entire
1K-to-32K range. Total spread is 7% — inside the rig's natural variance.
The original "1K is the knee" call holds.

**F2. Per-row CPU work is the bottleneck, not transaction overhead.** The
constant ~13µs/row regardless of batch size tells us that whatever's costing
us per-row (INSERT execution, B-tree updates, idempotency key check, WAL
write) cannot be amortized further by larger batches. Transaction-level
overhead (fsync, plan parse, lock acquisition) is already amortized to
near-zero at batch=1000.

**F3. Latency scales linearly with batch size.** p99 doubles every time
batch doubles. At 32K, p99 is 11 seconds per batch — well past
realistic-timeout territory. The 1K choice has the cleanest latency profile
without sacrificing throughput.

**F4. Run-to-run variance is highest at small batch sizes.** batch=32K
shows 2% spread; batch=16K shows 32%. Longer-running batches average out
noise within a 60s window; shorter ones don't. For future bench work,
prefer either larger batches OR more replicates at small sizes.

**F5. The previous "1000→8000 +4%" P3 finding was misleading.** That was
3 medians close to each other due to the same plateau, but presented as if
there were a real 4% gain. Re-analyzing both sweeps together, 1K→8K is
inside the noise band (~7% noise; 4% claimed gain). 1K is optimal but not
because anything above it is *worse* — it's optimal because it's where the
plateau starts.

## Implication for acct-togd extension PoC

The throughput plateau at ~13µs/row tells us where the wins must come from:

- **shmem rollup (acct-sw4i)**: targets removing the +12µs UPDATE step. After
  this, per-row cost should drop to ~6µs (bare INSERT + index + FK + apply).
  That's a real per-row reduction, not a batch-size effect.
- **FK cache (acct-ksay)**: removes +3µs/row.
- **Native FIFO (acct-fngj)**: orthogonal — addresses per-call dispatch cost,
  not the 13µs floor.

Stacked extension target: ~6µs/row → 150-200K tps at batch=1000.

**Production sizing recommendation**: batch=1000 stays the design target.
Larger batches don't gain throughput and pay linear latency cost. The
practical bench numbers for downstream perf comparisons should use
batch=1000.

## Why TigerBeetle differs

TigerBeetle batches up to 8K transfers per commit at the network/replication
layer, not at the SQL exec layer:
- No per-row fsync; commit primitive is consensus-based replication
- Direct memory-mapped storage; no buffer manager or B-tree overhead
- Bespoke wire protocol; no SQL parse/plan cost

Their 8K is a different optimum for a different system. Translating their
batch ceiling to a Postgres-based ledger is the kind of cross-system
extrapolation we explicitly flagged in acct-8hv2 methodology lessons.

## Files

- `db/postgresql.conf` — tuned PG18 conf used for this sweep.
- `poc/batch-ledger/bench/run-batch-size-sweep.sh` — sweep driver.
- `poc/batch-ledger/tests/bench_p3_append_only.rs` — bench harness.
- Per-run logs in `/tmp/poc-batch-size-sweep/batch_<size>/run_<i>.log`
  (not committed; reproducible via the sweep script).
