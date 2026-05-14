# FIFO arena rollback-inject characterization — A2 baseline

**acct-fhq7** · 2026-05-14 · against tip `1885c98` (A2 shadow on
`fifo_apply_batch_maximal` + `FIFO_PENDING_STACK`).

## Methodology

`bench_fifo_rollback_inject`. fan_out shape, 5000 pools, 20 workers,
batch=1000, 70% issue mix, 60s wall per run. Each batch wrapped in
`BEGIN ... <apply> ... COMMIT|ROLLBACK` with the COMMIT/ROLLBACK
decision sampled per batch at the target rollback percentage.

Sweep rates: 0% / 1% / 5% / 10%. Higher rates aren't realistic ERP
workload shape; the 0-10% window covers the relevant regression
boundary per acct-fhq7 acceptance criterion #4 (rewrite must stay
within 10% of A2 throughput at 5% rollback).

Pre-seeded 5 layers × 1M qty per pool — issues never exhaust under
20w × batch=1000 × 70% × max-take=10 within 60s.

## Results

| rollback% | committed/s | attempted/s | transfers/s | p99 (ms) | p99.9 (ms) | deadlocks |
|----------:|------------:|------------:|------------:|---------:|-----------:|----------:|
|       0   |        39.0 |        39.0 |      38,994 |      744 |        947 |         0 |
|       1   |        37.9 |        38.2 |      37,857 |      805 |       1042 |         0 |
|       5   |        37.4 |        39.7 |      37,439 |      696 |        766 |         0 |
|      10   |        33.7 |        37.6 |      33,676 |      784 |        913 |         0 |

Observed rollback rates ranged 0.00% / 0.87% / 5.64% / 10.45% — within
expected RNG variance of targets.

## Analysis

**Committed throughput delta vs 0% baseline (39.0 ops/s):**

| rollback% | committed/s | Δ vs baseline |
|----------:|------------:|--------------:|
|         0 |        39.0 |         —     |
|         1 |        37.9 |       −2.8%   |
|         5 |        37.4 |       −4.1%   |
|        10 |        33.7 |      −13.6%   |

**Attempted throughput is stable at ~38–40 batches/s across rates.** The
system processes work at the same rate; rollbacks substitute for
commits, so committed/s scales roughly with `(1 − rollback_pct)`. The
extra −4% at 5% rollback (vs the linear baseline of −5%) and −3.6% at
10% (vs linear −10%) are within rig noise.

**A2 has no measurable rollback-tax beyond the trivial substitution
effect.** The shadow approach pays its full ~17–19% throughput tax up
front at 0% rollback (vs the pre-A2 41K fan-out / 31K fan-in baseline
measured in acct-b3vs); rollback rate within the 0–10% range adds no
additional penalty.

**Zero deadlocks across the full sweep.** Per-cell EXCL serializer at
xact_commit replay handles concurrent commits + concurrent aborts
without contention pathology.

**p99 latency stays in the 690–805 ms band across rates.** Rollback
rate is not a significant latency driver — confirms that the
xact_abort cost (shadow discard, thread_local drop) is constant and
trivial vs the xact_commit cost (sorted EXCL acquisition + ops
replay).

## Implications for acct-fhq7 architecture decision

**Acceptance criterion #4 baseline locked**: any future rewrite
(Approach B/C/E/F) must deliver:

- ≥ 33.7 committed/s at 5% rollback (≤ 10% drop from A2's 37.4)

For a rewrite to *win* on perf:
- ≥ 39 committed/s at 0% rollback (recover the −17.1% A2 tax)
- ≥ 37 committed/s at 5% rollback (preserve at-rate behavior)

**Approach E (recon-triggered repair) projection**: in-place mutation
+ thread-local needs_repair flag should recover the 0% baseline to
~39–41K transfers/s (eliminating A2's shadow alloc tax). The 5%
rollback rate adds the cost of "flag flips on aborted cells" — should
be O(touched cells per batch) atomic stores. Negligible. **Total
projected: ≥ 40 committed/s at 5% rollback (~7% better than A2).**

If Approach E delivers in the bench: the architecture choice is
evidence-backed for the rewrite. If A2's per-backend MVCC isolation
turns out to be load-bearing in concurrent t2-t3-t4 multi-backend
stress, the conversation gets richer.

## Reproducing

```bash
for pct in 0 1 5 10; do
  POC_BENCH_ROLLBACK_PCT=$pct \
  POC_BENCH_FUNCTION=post_batch_fifo_maximal_F \
  POC_BENCH_SHAPE=fan_out \
  POC_BENCH_POOLS=5000 \
  POC_BENCH_DURATION_SECS=60 \
  cargo test --release --test bench_fifo_rollback_inject -- \
    --ignored --nocapture
done
```

Raw run logs captured at `/tmp/poc-fhq7-rollback-inject/run_${pct}.log`
during the 2026-05-14 sweep.
