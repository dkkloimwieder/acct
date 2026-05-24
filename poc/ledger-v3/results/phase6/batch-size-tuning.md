# acct-e5fz Part A — batch_size_max tuning sweep

**Issue:** acct-e5fz (P2; Part A of two)
**Run window:** 2026-05-24
**Bench:** `results/phase6/bench-e5fz/run-2026-05-24T04-08-21Z.log`

## Headline

**Keep the default `ledger_routed.batch_size_max = 50` as-is.** The sweep across `{50, 200, 1000, 5000}` on s2/s4/s5 routed AllWac surfaces a per-workload tension the bd description anticipated but the v21-inherited "raise to 5000" precedent does not address: **larger caps catastrophically degrade complex workloads (s4 throughput drops 46% at bsm=200) while only modestly helping the single-hot-pool case (s5 +25% at bsm=5000)**. The conservative default is the lowest-regret pick across the three measured workload shapes.

A more interesting redesign — `batch_window_us` as the primary cap with `batch_size_max` as a safety bound — is the natural follow-up; the sweep results below give it strong empirical motivation. Filed as **acct-w2dn** for Part B follow-up.

## Sweep results — 4 sizes × 3 scenarios, routed AllWac, default GUCs (except batch_size_max)

20 callers, 30s duration, 1000-pool universe. Per-cell JSONs in `results/phase6/bench-e5fz/`.

### s2 (zipf-1.5 simple) — flat across all bsm

| bsm  | tx/s     | ack p99 (ms) | committed p99 (s) | commit_group_avg | pipeline_ns_avg (ms) | drains |
|------|---------:|-------------:|------------------:|-----------------:|--------------------:|-------:|
| 50   | 1 307.9  |         62.8 |             13.87 |             7.23 |               14.03 |  8 207 |
| 200  | 1 362.1  |         58.0 |              8.02 |             7.43 |                8.62 | 11 305 |
| 1000 | 1 347.9  |         53.1 |              7.66 |             7.47 |                8.01 | 11 692 |
| 5000 | 1 308.7  |         56.4 |              7.61 |             7.41 |                8.21 | 11 517 |

**Throughput is flat (±4%) across all bsm values.** commit_group_avg stays at 7.2–7.5 regardless of cap — the workload is dispersed enough that windows close on time before any pool accumulates 50+ submissions. The cap doesn't bind at bsm=50; raising it changes nothing for this shape. bsm=200 is the bench-noise winner; bsm=50 is within 4%.

### s4 (zipf-1.2 complex) — bsm=50 dominates; larger caps catastrophically degrade

| bsm  | tx/s     | ack p99 (ms) | committed p99 (s) | commit_group_avg | pipeline_ns_avg (ms) | drains |
|------|---------:|-------------:|------------------:|-----------------:|--------------------:|-------:|
| 50   |   450.2  |        262.5 |             58.15 |            30.97 |              716.24 |    330 |
| 200  |   242.4  |        803.7 |             31.99 |            56.98 |            1 320.87 |    100 |
| 1000 |   214.1  |      1 187.0 |             29.56 |            75.85 |            2 051.84 |     58 |
| 5000 |   230.9  |      1 083.2 |             29.95 |            73.70 |            1 688.23 |     72 |

**bsm=50 is 2× faster than every other value.** Increasing the cap collapses throughput by 46% (bsm=200) → 52% (bsm=1000). The mechanism: s4 submissions are complex (many lines per submission), and the committer's per-commit_group `pipeline_ns_avg` scales superlinearly with `cg_size × lines_per_submission` — bsm=50 caps at 716ms per drain; bsm=1000 balloons to 2.05s. Lock-hold time on the contended pool tail blocks subsequent commit_groups; throughput is starved by serialization, not gated by per-commit cost.

The cap at 50 is the operative throughput floor. The fewer drains (58 vs 330) confirm the committer is the bottleneck: larger batches mean fewer batches but each takes much longer.

### s5 (single hot pool simple) — monotonic throughput at heavy latency cost

| bsm  | tx/s     | ack p99 (ms) | committed p99 (s) | commit_group_avg | pipeline_ns_avg (ms) | drains |
|------|---------:|-------------:|------------------:|-----------------:|--------------------:|-------:|
| 50   | 1 787.4  |         58.4 |             14.45 |            37.75 |              116.50 |  1 422 |
| 200  | 1 926.5  |        141.3 |             12.58 |           108.79 |              302.22 |    532 |
| 1000 | 2 151.8  |        241.7 |             10.56 |           161.58 |              380.68 |    401 |
| 5000 | 2 232.2  |        245.0 |             10.08 |           172.90 |              392.79 |    389 |

**bsm=5000 wins throughput by 25%** (2232 vs 1787) but **p99 ack latency degrades 4×** (245ms vs 58ms). The hot-pool workload is the case where larger batches help: aywu's chunking bypass + tm09's predecessor-wait force serialization through one pool, so amortizing per-COMMIT fsync over a larger commit_group is pure win on throughput. But every submission's ack waits until its whole 173-submission commit_group commits.

The marginal returns flatten between bsm=1000 (2152) and bsm=5000 (2232) — only 4% additional throughput for the same latency. Diminishing returns set in by ~1000.

## Why the conservative default holds

The bd description anticipated that "larger batch_size_max for WAC = more amortization = higher throughput, at the cost of larger per-commit memory + lock-hold time + arena footprint." The sweep numbers concretize the cost in two distinct cells:

1. **s4 lock-hold time** is not a marginal cost — at bsm=200 it cuts throughput in half. Complex submissions ↔ multi-line per submission ↔ pipeline_ns grows superlinearly. The default protects this case.

2. **s5 latency** quadruples when chasing the 25% throughput win. Tail latency matters for caller-facing flows; the 25% throughput in exchange for 4× tail latency is a poor trade for the typical ERP submission shape.

The v21 m8-ceiling bench that "raised from 50 default" optimized a single-pool throughput-only target with no latency budget; the trade was sensible there and counterproductive here. **PoC v3's `batch_size_max=50` default is correct; v21's m8-ceiling number is not transferable.**

## Recommendation

**No change to the GUC default.** `ledger_routed.batch_size_max` stays at 50.

The sweep does motivate Part B (`batch_window_us` as primary, `batch_size_max` as soft safety bound) more strongly than the bd description suggested:

- The dramatic s4 cliff at bsm=200 happens because the cap stops protecting against pipeline_ns runaway once the workload's natural commit_group size exceeds 50.
- A time-window-primary emit decision would cap commit_groups by wall-clock instead of by submission count, naturally preventing s4's lock-hold accumulation while still allowing s5-shaped workloads to grow commit_groups to their natural window size (which would land between bsm=200 and bsm=1000 for s5 based on the cg_avg progression).
- Part B's mitigation of acct-aywu's "open question" (hot-pool monopolization via unbounded growth under per-pool no-split) is also addressed by a time-window primary cap.

Part B is filed as **acct-w2dn** (P2) per the strict one-issue-at-a-time rule.

## What this DOES NOT change

- **No ledger-routed source changes.** Default GUC stays at 50; router emit logic unchanged.
- **No equivalence-correctness changes.** Cross-path equivalence under cm=4 is independent of batch_size_max — aywu and tm09 are the load-bearing pieces.
- **No bench infrastructure changes** beyond the new sweep script.

## Reproducing

```bash
# Full sweep (4 sizes × 3 scenarios × 30s + restart overhead = ~10 min)
bash poc/ledger-v3/scripts/run-bench-e5fz.sh

# Single cell — set GUC + bench manually
docker exec acct-postgres psql -U acct -d poc_v3 \
  -c "ALTER SYSTEM SET ledger_routed.batch_size_max = 1000;" \
  -c "SELECT pg_reload_conf();"
cargo run --release -p ledger-harness -- run \
  --scenario s5 --path routed --duration 30s \
  --method-mix all-wac --seed-count 1000 --max-callers 20 --no-sampler
# Restore
docker exec acct-postgres psql -U acct -d poc_v3 \
  -c "ALTER SYSTEM RESET ledger_routed.batch_size_max;" \
  -c "SELECT pg_reload_conf();"
```

`batch_size_max` is a `Sighup` GUC (lib.rs:214) — `ALTER SYSTEM SET` + `pg_reload_conf()` suffices; no postmaster restart needed.

## Cross-references

- **acct-aywu** (closed 2026-05-23) — per-pool no-split classifier on the router. This issue follows it: aywu means batch_size_max only affects WAC pools (order-sensitive methods bypass the chunking decision). The "open question" on hot-pool monopolization under per-pool no-split is scoped to **acct-w2dn** Part B.
- **acct-tm09** (closed 2026-05-23) — per-pool sequence numbers + committer predecessor-wait. Independent of batch_size_max; correctness-related not throughput-related.
- **acct-9mgx.1** (closed 2026-05-23) — surfaced the cm=1 throughput cost that originally motivated revisiting batch_size_max.
- **acct-w2dn** (open, this issue's Part B) — reframe `batch_window_us` as primary cap, `batch_size_max` as soft safety bound. Empirical motivation strengthened by this sweep's s4 cliff + s5 latency-vs-throughput trade.
- **v21 ceiling bench** (`poc/queue-extension-v21/bench/results-m8-ceiling/README.md`) — the "raise to 5000" precedent that motivated this re-evaluation. Optimized single-pool throughput-only with no latency budget; sets a misleading baseline for v3 multi-workload tuning.
