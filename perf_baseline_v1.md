# Perf baseline v1 — current schema (32 migrations), pre-Slice-A

**Date:** 2026-05-01 (driver started 15:35, finished 19:04 = ~3h29m wall clock)
**Schema:** 32 migrations through `0032_cost_adjust_retroactive`
**bd issue:** acct-ok2 (foundation audit)
**Methodology:** identical to `perf_baseline_v0.md` — 3 runs × 300 s per shape, vmstat sidecar. Goal: detect any regression introduced by the 11 migrations added since v0 was measured (0022 inventory_adjustments → 0032 cost_adjust_retroactive).

## TL;DR

12 of 13 shapes are flat-or-improved vs v0. **Shape A (1 writer × 5–20 events, shared credit) shows a 27 % drop** in events/s — just outside v0's documented 15–20 % single-day noise band. Several shapes (E, H, F) post implausibly large positive deltas (+87 %, +80 %, +18 %), which suggests v0's measurements for those shapes had unfavorable background conditions (consumer-laptop rig, single-day baseline). The comparison is directionally informative but not statistically rigorous on its own — two single-day baselines on a noisy rig.

**Recommendation:** Foundation is not catastrophically regressed. Shape A's drop deserves a targeted re-measurement using the `acct-ezm` 5 × 60 s methodology (less long-tail bias than 3 × 300 s on this rig) before Slice A merges; if the drop reproduces, root-cause the additional cost in `_post_transfers_compute_amount`'s shipped branches. Don't gate Slice A on this audit — proceed with the bench result as a starting line and re-measure once Slice A's shape is settled.

## Headline — cross-shape medians (v0 vs v1)

`v0` and `v1` columns are events/s medians (commit-side for the outbox shapes G/J/K/L/M; caller-side for the rest). `Δ%` is `(v1 − v0) / v0 × 100`.

| Shape | Description | v0 evps | v1 evps | Δ% | Note |
|---|---|---|---|---|---|
| **A** | 1 × 5–20  · shared credit | 2 164 | 1 580 | **−27 %** | Outside v0 noise band; needs ezm-style re-bench |
| **B** | 1 × 1000  · shared credit | 2 486 | 2 749 | +11 % | Within noise |
| **C** | 32 × 5–20 · shared credit | 1 421 | 1 464 | +3 % | Within noise |
| **D** | 100 × 5–20 · shared credit (worst case) | 559 | 596 | +7 % | Within noise |
| **E** | 100 × 100 · shared credit (anti-pattern) | 373 | 698 | +87 % | Implausible improvement; v0 likely had bad day |
| **F** | 100 × 5–20 · cross-account spread | 1 274 | 1 500 | +18 % | Borderline noise band |
| **G** | 100 × 5–20 · outbox + 1 drainer | 140 | 211 | +50 % | Real or v0 noise; 50 % is suggestive |
| **H** | 70 + 30 · qty + reserve interleave | 1 186 | 2 130 | +80 % | Implausible improvement; same caveat as E |
| **I** | 50 + 50 · qty + multi-cur value | 2 840 | 3 249 | +14 % | Within noise |
| **J** | 100 × 5–20 · super-batch outbox + 1 drainer | 365 | 461 | +26 % | Outside noise (positive) |
| **K** | 100 × 5–20 · super-batch outbox + 4 drainers | 180 | 203 | +13 % | Within noise |
| **L** | 100 × 5–20 · pseudo-sync via LISTEN/NOTIFY | 2 876 | 3 258 | +13 % | Within noise — peak still L |
| **M** | 100 × 5–20 · async outbox + back-pressure cap | 971 | 1 125 | +16 % | Borderline noise band |

## Per-shape latency (v1 only — for record)

p50 / p95 / p99 in milliseconds. Source: `/tmp/perf_v1_run/<shape>/driver.log` "Cross-config medians" sections, plus per-config-detail tables for outbox commit metrics.

| Shape | p50 ms | p95 ms | p99 ms | bps | WAL MB |
|---|---|---|---|---|---|
| A | 7.1 | 13.1 | 14.6 | 126.2 | 442 |
| B | 333.5 | 482.6 | 627.2 | 2.75 | 809 |
| C | 155.2 | 906.5 | 1 381.6 | 117.2 | 420 |
| D | 1 134.4 | 7 386.3 | 11 373.5 | 47.7 | 175 |
| E | 12 690.6 | 28 940.6 | 34 987.9 | 7.0 | 218 |
| F | 44.7 | 4 947.0 | 7 850.4 | 119.9 | 509 |
| G | 46.6 (enqueue) | 54.3 | 63.2 | 2 117 enqueue / 16.9 commit | 1 047 |
| H | 6.2 | 86.6 | 3 432.7 | 905.2 | 638 |
| I | 64.3 | 1 576.3 | 2 155.4 | 259.5 | 1 003 |
| J | 54.0 (enqueue) | 61.3 | 71.1 | 1 822 enqueue / 36.9 commit | 1 070 |
| K | 53.4 (enqueue) | 61.1 | 70.0 | 1 837 enqueue / 16.3 commit | 793 |
| L | 16.1 (caller end-to-end) | 22.5 | 25.5 | 260.8 | 1 091 |
| M | 43.1 | 94.1 | 102.8 | 90.2 | 511 |

## Methodology notes

- **3 runs × 300 s per shape**, identical to v0. Each run preceded by an ephemeral test-DB recreate so cold-cache effects show up consistently.
- **Hardware**: same dev laptop as v0 (consumer kernel; thermal/scheduling jitter dominates at percentile tails — see v0 caveats).
- **Software state**: 32 migrations applied (v0 had 21). The new migrations introduce: `inventory_adjustments` table + function (0022), `wac_variant_split` enum work (0023), `inventory_cost_adjustments` (0024), `transfers_provisional` (0025), `close_period` orchestrator (0026), `standard_costs` separate entity + `post_standard_cost_roll` (0027–0028), `wac_periodic` real body (0029), `transfers.qty` column + per-class WAC divisor refactor (0030), `wac_retroactive` real body (0031), `inventory_cost_adjustments_retroactive` + `cost_adjust_retroactive_hook` real body (0032).
- **What actually changed in `post_transfers`**: 0029 added `wac_periodic` to the `v_has_wac` trigger and provisional flagging; 0030 added the `qty` column INSERT path and refactored the WAC divisor; 0031 added `wac_retroactive` to the same code paths. The non-WAC single-pass branch is largely unchanged. Shapes A–E/F (which exercise mostly the non-WAC straight-through path) shouldn't have been hit hard by these changes.

## Why is Shape A down 27%?

Hypotheses, ordered by likelihood:

1. **Background load drift between v0 and v1 measurement days** — most likely. v0 was run 2026-04-29/30; v1 was 2026-05-01. The acct-ezm methodology (5 × 60 s with cooldowns) was specifically built to characterize this rig's day-to-day noise; it found the band is wider than a 3 × 300 s methodology suggests.
2. **Added function-resolution overhead in `_post_transfers_compute_amount`** — the dispatcher now has 4 real branches (was 2 in v0). For non-cost-relevant events (which is what Shape A posts — `bin_move` between two qty-side accounts of the same SKU), the dispatcher isn't called at all, so this shouldn't matter. Worth verifying.
3. **`transfers.qty` column INSERT cost** — 0030 added a new column; every INSERT now writes one more BIGINT. Even for non-inventory transfers (which set `qty := NULL`), there's a small-but-real INSERT-shape change. Plausible mild cost.
4. **`post_transfers` body length** — 1078 lines now (was ~600 in v0). PL/pgSQL plan caching may be slightly less effective. Should be tiny.

If Shape A's regression reproduces under ezm-style re-bench, hypotheses 3 and 4 are the candidates worth profiling. Shape B (single writer, 1000-event batch) is *not* regressed (+11 %), which argues against a per-INSERT cost — if every INSERT got slower, B would regress harder than A (1000 inserts per batch vs 5–20). So whatever's slower in A is per-batch overhead, not per-event. That points away from `transfers.qty` and toward some setup cost in `post_transfers` itself.

This is conjecture from one bench. **acct-ok2 doesn't try to root-cause it**; the audit's job is to confirm the foundation isn't catastrophically broken. It's not.

## Why E and H look implausibly improved

Shape E (anti-pattern: 100 writers × 100 events shared credit) and Shape H (qty + reservation interleave) both show +80–87 % improvement. These shapes have wider tails (more sensitive to background OS scheduling) and were measured back-to-back with several other shapes on the v0 day. It's likely v0's runs for those shapes hit unlucky scheduler decisions or thermal events. Two-day comparisons aren't strong enough to distinguish "real improvement" from "v0 had a bad day on these shapes."

The headline takeaway for these shapes is: **L is still the throughput peak** at 3 258 commit-evps in v1 (slightly up from v0's 2 876). Pseudo-sync remains the documented escape hatch from sync `post_transfers`.

## Re-running this baseline

```bash
./scripts/run-perf-baseline-v1.sh
```

Logs land in `/tmp/perf_v1_<timestamp>/<shape>/driver.log`. Each shape has its "Cross-config medians" headline section near the bottom, plus per-config detail above it (where commit_evps for outbox shapes lives). Aggregate medians by hand into the headline table here, or extend the script to emit a CSV summary.

For the targeted Shape-A re-measurement:

```bash
T4_BASELINE_RUNS=5 T4_DURATION_SECS=60 \
  T4_CONFIGS="1:5:20" \
  ./scripts/run-perf-baseline.sh
```

5 × 60 s with 30 s cooldowns is the `acct-ezm` methodology that established this rig's noise band as ~15–20 %. If Shape A median across those 5 runs is still ≥ 25 % below v0 baseline (2 164 evps), the regression is real and worth a profiling pass before Slice A.

## Reference data

- v0 baseline: `perf_baseline_v0.md` — 13-shape matrix on the 21-migration schema, measured 2026-04-29/30.
- v0 caveats section documents the rig noise honestly; same applies here.
- `acct-ezm` (closed): re-measurement methodology for noise characterization. Memory: `bd memories ezm-2026-04-30-no-regression-from-acct-uxu`.
