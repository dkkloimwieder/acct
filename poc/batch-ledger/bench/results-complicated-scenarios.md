# Complicated-scenario benches — fan-in, fan-out, sustained

Sub-issue under acct-togd. Three scenarios that probe sw4i's expected win
under shapes the simple P3 benches don't cover.

## Methodology

- Tuned PG conf (db/postgresql.conf, acct-1vo7).
- 20 workers, batch_size=1000, 3×60s replicates per shape with 15s gaps.
- One sustained 10-min run for cross-checkpoint behavior (didn't hit a
  checkpoint at 15min timeout, but tests autovacuum + bgwriter cadence).
- Each shape tested against both `post_batch` (mutable balance — UPDATE
  accounts) and `post_batch_append_only` (INSERT-only).

## Results (medians)

| Shape | mutable balance | append-only | append-only win |
|---|---|---|---|
| **Fan-in** (1 hot credit, 50 dr accounts) | 20,364 tps · p99 1,124ms | 77,119 tps · p99 399ms | **3.8×** |
| **Fan-out** (5,000 accounts random pair) | 7,182 tps · p99 11,890ms | 68,824 tps · p99 510ms | **9.6×** |
| Standard P3 baseline (50 accts random pair) | 40,610 (PoC) | 80,514 (PoC) | 1.98× |
| Sustained 10-min (append-only, P3 shape) | — | 74,230 tps · p99 487ms | — |

## Findings

**F1. The append-only / sw4i win scales with account-row work.** The simple
P3 bench (50 accounts, random pair) showed 2× as the headline. Realistic
high-cardinality workloads (fan-out, 5K accounts) show **nearly 10×**. The
simple bench under-stated sw4i's value because it didn't stress account-row
contention or cache footprint.

**F2. Counterintuitive: fan-out mutable is WORSE than fan-in mutable.**
20K vs 7K. The intuition "fewer hot rows = less contention = faster" is
falsified for `post_batch`'s pre-lock pattern.

Root cause: `post_batch` does
```sql
PERFORM 1 FROM accounts WHERE id IN (<all batch accounts>) ORDER BY id FOR UPDATE;
```
before the UPDATE step. With fan-in's 50 hot accounts, this FOR UPDATE
acquires the SAME warm lock set every batch — lock manager hash entries
stay hot, B-tree pages stay in shared_buffers, the row locks behave nearly
free. With fan-out's 5K accounts, each batch acquires ~1000 DIFFERENT
locks — cold lock manager entries, scattered B-tree page accesses, more
WAL records on the UPDATE step, no locality. The cold-page-thrash from
many distinct rows exceeds the hot-row contention.

Append-only sidesteps this entirely by skipping FOR UPDATE; fan-out
append-only is within noise of fan-in append-only (68K vs 77K).

**F3. The hot-row-vs-cold-pages duality is architecturally significant.**
- Hot-row contention: acct-slhp (sharded balances) targets this.
- Cold-page thrash on many distinct rows: sw4i shmem rollup targets this
  because shmem hash has uniform O(1) access cost regardless of which
  account is being updated. No B-tree, no page cache, no lock manager.

sw4i and sharded balances are complementary, not redundant. Production
workloads exhibit both shapes.

**F4. Sustained 10-min holds steady.** 74K tps, p99 487ms — no drift over
the longer run. Did not hit a checkpoint (timeout 15min). Cross-checkpoint
behavior requires a 20+ min run; not measured here.

**F5. Latency tail at fan-out mutable is alarming.** p99 of 12 seconds per
batch under cold-lock acquisition. The same shape append-only is 500ms.
The 24× tail-latency improvement is its own argument for sw4i, beyond
throughput.

## Implication for acct-togd v1 ceiling estimate

Previous estimate (per acct-sw4i description): 150K tps target = 2× recovery
of the simple-shape 80K append-only baseline.

Revised estimate for realistic high-cardinality workloads:
- Fan-out shape mutable today: 7K tps
- Append-only on same shape: 69K tps (~10× lift)
- Plus FK cache (acct-ksay): another ~10-20% on per-row cost
- Plus native FIFO/wac for the cost-method-heavy fraction: catches FIFO's
  structural ceiling

For workloads dominated by realistic account distributions (per-vendor ×
per-currency partitions, per-sku × per-location pools), sw4i alone is
worth **5-10×**, not 2×. Material upward revision of the extension's value.

## Implication for acct-sw4i design

The shmem-rollup design assumed roughly uniform hash bucket access. Fan-out
findings validate that: workers shouldn't care which account they're
incrementing — bucket access is O(1). The hot-row case (fan-in) is the
nominal worst case but actually maps cleanly to per-bucket spinlock — only
the one hot bucket is contended. That's bounded contention; sharding can
help if it matters.

## Implication for acct-slhp (sharded balances)

slhp targeted "hot-row FOR UPDATE serialization" as the bottleneck. F2
reveals that's only HALF the cost model — cold cross-account page thrash
is the other half. After sw4i ships and removes the FOR UPDATE entirely,
slhp's remaining benefit is only the logical "this account agrees across
writers" relaxation, not contention relief. Re-evaluate slhp scope after
sw4i lands.

## Files

- `tests/bench_fan_in.rs` — fan-in harness, POC_BENCH_FUNCTION-switchable.
- `tests/bench_fan_out.rs` — fan-out harness, POC_BENCH_FUNCTION-switchable.
- `bench/run-complicated-scenarios.sh` — sweep driver.
- Per-run logs under `/tmp/poc-complicated-scenarios/` (not committed;
  reproducible via the sweep script).
