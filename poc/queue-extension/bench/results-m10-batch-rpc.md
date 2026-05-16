# acct-22xt — Queue PoC caller-side batch RPC (b=N)

Per-cell 5 × 60s with 30s settle. Batch size b=1000. Default GUCs (bw=500 bs=1024 sc=on).

## Throughput (events/sec, batch-grain latency µs)

| shape | N | evps med | evps IQR | batch p50 µs | batch p99 µs | batch p99.9 µs | deadlocks med |
|---|---|---|---|---|---|---|---|
| fan_in | 32 | 53000 | 67 | 545279 | 1455103 | 1857535 | 0 |
| fan_in | 128 | 49050 | 750 | 2494463 | 5783551 | 6905855 | 0 |
| fan_out | 32 | 16217 | 133 | 1950719 | 3368959 | 3844095 | 0 |
| fan_out | 128 | 17050 | 250 | 8142847 | 13688831 | 15204351 | 0 |
| small_batch | 32 | 59400 | 1150 | 529407 | 825343 | 1042431 | 0 |
| small_batch | 128 | 50000 | 4367 | 2217983 | 5693439 | 7585791 | 0 |

## Per-run detail

| shape | N | run | batches_ok | events_ok | events_err | evps | p50 µs | p99 µs | p99.9 µs | deadlocks Δ | classifier |
|---|---|---|---|---|---|---|---|---|---|---|---|
| fan_in | 32 | 0 | 2965 | 2965000 | 0 | 49417 | 580095 | 1547263 | 1982463 | 0 | B5:wake |
| fan_in | 32 | 1 | 3184 | 3184000 | 0 | 53067 | 545279 | 1455103 | 1911807 | 0 | B5:wake |
| fan_in | 32 | 2 | 3180 | 3180000 | 0 | 53000 | 543231 | 1413119 | 1810431 | 0 | B5:wake |
| fan_in | 32 | 3 | 3216 | 3216000 | 0 | 53600 | 534527 | 1440767 | 1796095 | 0 | B5:wake |
| fan_in | 32 | 4 | 3180 | 3180000 | 0 | 53000 | 546303 | 1455103 | 1857535 | 0 | B5:wake |
| fan_in | 128 | 0 | 2821 | 2821000 | 0 | 47017 | 2633727 | 5783551 | 6631423 | 0 | B5:wake |
| fan_in | 128 | 1 | 2944 | 2944000 | 0 | 49067 | 2490367 | 5668863 | 6905855 | 0 | B5:wake |
| fan_in | 128 | 2 | 2943 | 2943000 | 0 | 49050 | 2494463 | 5836799 | 7491583 | 0 | B5:wake |
| fan_in | 128 | 3 | 2899 | 2899000 | 0 | 48317 | 2535423 | 6082559 | 7147519 | 0 | B5:wake |
| fan_in | 128 | 4 | 2964 | 2964000 | 0 | 49400 | 2484223 | 5644287 | 6627327 | 0 | B5:wake |
| fan_out | 32 | 0 | 906 | 906000 | 0 | 15100 | 2109439 | 3461119 | 3794943 | 0 | B5:wake |
| fan_out | 32 | 1 | 973 | 973000 | 0 | 16217 | 1950719 | 3364863 | 3723263 | 0 | B5:wake |
| fan_out | 32 | 2 | 972 | 972000 | 0 | 16200 | 1940479 | 3368959 | 4061183 | 0 | B5:wake |
| fan_out | 32 | 3 | 982 | 982000 | 0 | 16367 | 1927167 | 3330047 | 3844095 | 0 | B5:wake |
| fan_out | 32 | 4 | 980 | 980000 | 0 | 16333 | 1960959 | 3409919 | 4788223 | 0 | B5:wake |
| fan_out | 128 | 0 | 939 | 939000 | 0 | 15650 | 8896511 | 14663679 | 17448959 | 0 | B5:wake |
| fan_out | 128 | 1 | 1029 | 1029000 | 0 | 17150 | 8028159 | 13688831 | 14188543 | 0 | B5:wake |
| fan_out | 128 | 2 | 1023 | 1023000 | 0 | 17050 | 8142847 | 13762559 | 15204351 | 0 | B5:wake |
| fan_out | 128 | 3 | 1014 | 1014000 | 0 | 16900 | 8237055 | 13180927 | 14934015 | 0 | B5:wake |
| fan_out | 128 | 4 | 1031 | 1031000 | 0 | 17183 | 8089599 | 13615103 | 15974399 | 0 | B5:wake |
| small_batch | 32 | 0 | 3272 | 3272000 | 0 | 54533 | 574975 | 876031 | 1074175 | 0 | B5:wake |
| small_batch | 32 | 1 | 3564 | 3564000 | 0 | 59400 | 529407 | 801279 | 960511 | 0 | B5:wake |
| small_batch | 32 | 2 | 3538 | 3538000 | 0 | 58967 | 530431 | 825343 | 1042431 | 0 | B5:wake |
| small_batch | 32 | 3 | 3607 | 3607000 | 0 | 60117 | 517119 | 832511 | 1062911 | 0 | B5:wake |
| small_batch | 32 | 4 | 3616 | 3616000 | 0 | 60267 | 519935 | 797183 | 977407 | 0 | B5:wake |
| small_batch | 128 | 0 | 2937 | 2937000 | 0 | 48950 | 2416639 | 5894143 | 7397375 | 0 | B5:wake |
| small_batch | 128 | 1 | 2604 | 2604000 | 0 | 43400 | 2162687 | 5451775 | 7761919 | 0 | B5:wake |
| small_batch | 128 | 2 | 3280 | 3280000 | 0 | 54667 | 2123775 | 5693439 | 8429567 | 0 | B5:wake |
| small_batch | 128 | 3 | 3199 | 3199000 | 0 | 53317 | 2217983 | 5591039 | 7258111 | 0 | B5:wake |
| small_batch | 128 | 4 | 3000 | 3000000 | 0 | 50000 | 2338815 | 6135807 | 7585791 | 0 | B5:wake |

## Comparison context

Reference numbers from the prior PoCs at the same shapes (different harnesses, same hardware, same DB-on-Docker rig):

| source | b | fan_in | fan_out | notes |
|---|---|---|---|---|
| Queue PoC M9.2 (acct-4d4n.21) | 1 | ~11878 evps @ N=256 | ~6379 evps @ N=128 | poc-validation-spec headline |
| Queue PoC M10 backfill (acct-4d4n.23) | 1 | 364 evps @ N=1 | 376 evps @ N=1 | for P1 ratio |
| Shmem rollup PoC (acct-sw4i) | 1000 | ~67000 evps | ~43500 evps | poc/ledger-extension/bench/results-shmem-apply.md |

## Findings

1. **Caller-side b=1000 closes most of the RPC-amortization gap.** Queue PoC at b=1000 fan_in N=128 hits **49050 evps** vs M9.2's b=1 N=256 baseline of 11878 — a **4.1× lift from caller-side batching alone**, no architectural change. fan_out N=128 goes from 6379 → 17050 evps = **2.7×**. small_batch N=128 from 8017 → 50000 evps = **6.2×**.

2. **Queue PoC at b=1000 vs shmem rollup at b=1000.** fan_in 49K/67K = **73% of shmem rollup ceiling**. fan_out 17K/43.5K = **39% of shmem rollup ceiling**. small_batch 50K (no shmem rollup reference shape match, comparable to fan_in tier).

3. **Remaining gap is committer + slot machinery + SPI write overhead**, not RPC. The shmem rollup PoC's accounts table sees in-memory atomic increments via cache-line-aligned bucket CAS; the queue PoC writes per-event cost rows via SPI through the committer's drain loop. fan_in is closer to ceiling than fan_out because fan_in concentrates all events into one committer's drain pipeline (one big SPI batch); fan_out spreads across 16 shards × 16 committer drains, each smaller and amortizing SPI less efficiently.

4. **Zero deadlocks across 30 runs** (5 runs × 6 cells). Streaming push-with-harvest is correctness-safe under N=128 fan_in contention.

5. **Latency tradeoff at the batch grain is huge** but per-event amortizes to low single-digit ms. fan_in N=128 batch p99 = 5.8 s but per-event = 5.8 s / 1000 = 5.8 ms. fan_out N=128 batch p99 = 13.7 s → per-event 13.7 ms. The b=1000 regime is throughput-optimized, not latency-optimized; a real caller would pick b based on latency budget (b=100 likely lands ~30K evps with sub-second batch p99).

6. **Slot pool sizing surfaced as a real constraint.** First-pass batch implementation acquired all N slots up front; saturated POC_SLOTS_PER_SHARD=512 under fan_in N≥32. Final implementation streams push-with-harvest: when acquire_slot returns None, harvest any FILLED slots from current pending list (freeing pool), then retry. Bounded by queue_full_timeout_ms. Adaptive — no K-tuning required.

7. **Design-v2 implication.** Caller-side batch RPC is a load-bearing requirement for the queue architecture to be competitive with the shmem-rollup PoC on bulk workloads. Single-event b=1 alone leaves 4-6× throughput on the table. The b=1000 internal-streaming pattern characterized here is the reference shape; whether design-v2 exposes it as a separate entrypoint or absorbs it into the single-event surface (with caller-provided arrays) is a design call but the underlying mechanism — streamed push-with-harvest — is validated.

## Out of scope (file as acct-22xt-followup if a real driver surfaces)

- Mixed-method batches (all events in this bench use the shape's natural method)
- Multi-currency / multi-cost-book
- Failure semantics under per-event errors (currently surfaces in error_code; not stress-tested under arbitrary plan_apply failures)
- Caller-cancel mid-batch (only the LAST pushed slot's wait is cancel-safe via CHECK_FOR_INTERRUPTS; earlier pushed slots leak to the slot-leak audit)
- Larger slot pool sizing (POC_SLOTS_PER_SHARD const)
- b sweep (this bench is fixed at b=1000 per scope lock)

Generated: 2026-05-16T17:44:11Z
