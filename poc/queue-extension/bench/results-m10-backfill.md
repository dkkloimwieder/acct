# M10.1 (acct-4d4n.23) — P1/P3 backfill

Per-cell 5 × 60s with 30s settle gap. Default GUCs: bw=500 bs=1024 sc=on.

## fan_out cells

| N | tps med | tps IQR | p50 med µs | p99 med µs | p99 IQR µs | p99.9 med µs | deadlocks med |
|---|---|---|---|---|---|---|---|
| 1 | 376 | 0 | 2617 | 3445 | 54 | 6647 | 0 |
| 16 | 3325 | 508 | 4091 | 15695 | 3688 | 27935 | 0 |

## Per-run detail

| N | run | applies | errors | tps | p50 µs | p99 µs | p99.9 µs | deadlocks Δ | classifier |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0 | 22449 | 0 | 374 | 2625 | 3495 | 6727 | 0 | idle |
| 1 | 1 | 22545 | 0 | 376 | 2617 | 3445 | 6723 | 0 | idle |
| 1 | 2 | 22538 | 0 | 376 | 2625 | 3367 | 6383 | 0 | idle |
| 1 | 3 | 22544 | 0 | 376 | 2615 | 3447 | 6639 | 0 | idle |
| 1 | 4 | 22599 | 0 | 377 | 2615 | 3393 | 6647 | 0 | idle |
| 16 | 0 | 199493 | 0 | 3325 | 4091 | 15695 | 27935 | 0 | idle |
| 16 | 1 | 182958 | 0 | 3049 | 4267 | 17743 | 31727 | 0 | idle |
| 16 | 2 | 182902 | 0 | 3048 | 4259 | 17727 | 32415 | 0 | idle |
| 16 | 3 | 213456 | 0 | 3558 | 4001 | 14039 | 25407 | 0 | idle |
| 16 | 4 | 222467 | 0 | 3708 | 3973 | 13383 | 23967 | 0 | idle |

## Criteria evaluation

- **P1** (fan_out N=32 ≥ 24× N=1): N=32 best from M9.3 ≈ 5337 tps vs N=1 baseline 376 tps; ratio = **14.2×**. **FAIL**
- **P3** (fan_out N=16 p99 < 50ms): measured p99 = **15.7 ms**. **PASS**

Generated: 2026-05-16T15:46:21Z
