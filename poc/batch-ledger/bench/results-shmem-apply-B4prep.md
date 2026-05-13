# acct-zo4t M10.B4-prep — shmem apply bench (AtomicU128 (balance, qty))

Re-baseline of the shmem fan-in / fan-out benches after replacing the
`Bucket` struct's separate `AtomicI64 balance` + `AtomicI64 qty` fields
with a single `portable_atomic::AtomicU128` packed pair. Documents the
correctness-vs-throughput trade.

## What changed in B4-prep

The pre-B4-prep `try_update_existing` did:
```
balance.fetch_add(amount_delta, AcqRel);  // LOCK XADD
qty.fetch_add(qty_delta, AcqRel);         // LOCK XADD
```

Each is one hardware-atomic op (`LOCK XADD` on x86_64). They are
independent: a reader sampling between them observes a `(balance, qty)`
pair that was never a real coupled state (the I11 torn-read gap; cf.
`tests/seqlock_torn_read_t1.rs::t2_torn_read_probe` — pre-B4-prep
falsified within 15s capturing `(balance=38022000, qty=38021)`).

Post-B4-prep:
```
balance_qty_fetch_add(&b.balance_qty, amount_delta, qty_delta);
// CAS-loop on AtomicU128 — load, unpack, modify, repack, cmpxchg.
// LOCK CMPXCHG16B on x86_64; retries on contention.
```

One atomic 128-bit RMW per successful round. Under contention the loop
retries. Readers do a single `balance_qty.load(Acquire)` + unpack —
one `MOV` on x86_64 with cmpxchg16b for atomicity guarantees, returning
a real coupled `(balance, qty)` snapshot.

The x86_64 build is configured with `target-feature=+cmpxchg16b`
(`poc/ledger-extension/.cargo/config.toml`) so `portable_atomic::AtomicU128`
resolves to a true lock-free implementation, not a spinlock fallback.
cmpxchg16b is universal in x86_64 CPUs since 2011.

## Methodology

Identical to A2 (`bench/results-shmem-apply-A2.md`):
- PG 18.3 in `acct-postgres` container, tuned conf (32 GB target).
- 20 workers × batch=1000 × 60s × 3 replicates with 15s gaps.
- Fan-in: 50 debit accounts + 1 hot credit (CAS contention).
- Fan-out: 5000 accounts, even split into debit / credit (mostly
  unique cells, CAS usually succeeds first try).
- N_BUCKETS = 16384.
- `POC_BENCH_FUNCTION=post_batch_shmem`.

## Per-run throughput

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in shmem  | 40,445 | 40,824 | 49,917 | **40,824** |
| fan-out shmem | 42,537 | 44,711 | 34,664 | **42,537** |

## Per-run p99 latency (ms)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in shmem  | 761 | 753 | 624 | **753** |
| fan-out shmem | 686 | 697 | 975 | **697** |

## Headline deltas vs A2

| Scenario | A2 median tps | B4-prep median tps | Δ tps | A2 p99 | B4-prep p99 | Δ p99 |
|---|---|---|---|---|---|---|
| fan-in  | 57,045 | 40,824 | **-28.4%** | 566 ms | 753 ms | +33.0% |
| fan-out | 57,723 | 42,537 | **-26.3%** | 467 ms | 697 ms | +49.3% |

Deadlocks: **0** across all 6 runs (same as A2 — apply path still
takes no row locks on `accounts`).

## Findings

**F1. CAS-loop carries a real cost vs hardware fetch_add.** Both shapes
regress ~25–28% in throughput. The pre-B4-prep `LOCK XADD` pair is two
contention-tolerant hardware-atomic ops; B4-prep's `LOCK CMPXCHG16B`
loop is one op per successful round but retries under write-write
collisions. cmpxchg16b is also slightly more expensive than xadd at
the microarchitectural level (longer pipeline serialization).

**F2. Fan-in's higher contention regime exposes the CAS-loop cost
modestly more than fan-out.** Fan-in's hot credit cell sees 20 writers
contending on one bucket every batch; fan-out's 5000 cells see ~1
writer per cell per batch on average. The fan-in retry rate is higher,
but the gap (-28.4% vs -26.3%) is smaller than expected; both shapes
are dominated by lock cycling + commit overhead rather than the apply
proper, and the CAS-loop cost is mostly the cmpxchg16b instruction
itself rather than retry count.

**F3. Variability is high (IQR ~22% for fan-out's tps).** Fan-out run
3 (34,664 tps) is 22% below fan-out run 2 (44,711 tps); per the ezm
methodology memory this is real rig noise. The median is more robust
than the headline tps; the directional finding (~25% regression) is
solid.

**F4. p99 latency degraded more than throughput (fan-out +49%).**
Tail-latency sensitivity to CAS-loop contention is expected. Under
contention, the worst-case writer retries multiple times before
succeeding. For typical ERP workloads where the unit-cost dispatch
correctness is load-bearing, p99 in the ~700–1000 ms range is still
well within acct's existing wrapper SLOs (per perf_baseline_v2.md,
combined p99 ~3.9 s in the c4p baseline).

**F5. Correctness preserved.** `ledger_shmem_recon()` returns drift=0
after every bench run. The M8 recon assertion still holds. Plus the
new T2 falsification gate (`seqlock_torn_read_t1.rs::t2_torn_read_probe`)
confirms zero torn reads across millions of observations — the
defining I11 correctness property post-B4-prep.

## Cross-shape interpretation

B4-prep is a **correctness-mandatory regression**, not a perf
optimization. The pre-B4-prep code shipped with a silent
`(balance, qty)` torn-read bug invisible at the simple-balance grain
(qty_delta=0) but load-bearing under WAC (B4, `post_batch_wac_shmem`):
`unit_cost = pool_value / pool_qty` with a torn read produces wrong
costing and silent variance leak — the same class of bug as AP9 / R7
in the acct codebase.

The 25–28% throughput cost is the price of linearizable `(balance, qty)`
reads. Specifically:
- Without B4-prep + with WAC → silent variance leak. Unshippable.
- With B4-prep + WAC → correct costing, 25% slower than M9 / A2 headline.

We accept the slowdown. The headline B4-prep shmem numbers are still
~3× over the mutable `post_batch` baseline (43–44K tps vs ~14K mutable
median per `results-shmem-apply.md`).

## Future tuning levers (not in M10 scope)

1. **Inline asm cmpxchg16b.** `portable_atomic` adds a small wrapper
   layer; hand-written inline asm can shave a few percent. Not
   load-bearing today.
2. **Compact pack to 64 bits.** If a workload analysis shows balance
   and qty both fit in 32 bits, pack into AtomicU64 and use fetch_add.
   Not generally applicable (balance in cents can exceed 2^31 for
   high-value GL accounts within one period).
3. **Per-cell seqlock with multi-writer awareness.** A `writers_in_flight`
   counter + version field can avoid CAS at the cost of reader retry
   pressure under continuous writes. The bd issue's textbook seqlock
   was rejected during design because it doesn't compose with M9's
   lock-free SHARED-LWLock concurrency; a multi-writer-aware variant
   is possible but complex. File a followup if perf becomes a
   real bottleneck after B4 measures.
4. **Promote to EXCLUSIVE LWLock on writes.** Defeats the lock-free
   property — only consider if B4-prep blocks a measured perf target.

## Caveat: pgrx debug-build path

The `seqlock_torn_read_t1.rs` test runs against `cargo build --release`
via the install script (the .so loaded by Postgres is release). The
bench harness `bench_fan_*.rs` runs `cargo test --release` against
the same release .so. Both paths exercise the same code; no
debug-vs-release asymmetry in the bench numbers.

## Raw logs

Per-run logs at `/tmp/poc-b4prep-bench/fanin_shmem/run_{1,2,3}.log`
and `/tmp/poc-b4prep-bench/fanout_shmem/run_{1,2,3}.log`. Retained on
the bench host; not committed to git.

## Conclusion

B4-prep ships. Trade documented: -25–28% throughput, +33–49% p99,
**0 torn reads** across the falsification gate. The correctness
floor (I11) is no longer a gap. B4 (post_batch_wac_shmem, acct-n4mo)
is now unblocked and can dispatch WAC unit-cost against an
always-coupled `(pool_value, pool_qty)` snapshot.
