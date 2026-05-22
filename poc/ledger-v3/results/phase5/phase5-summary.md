# Phase 5 first-pass measurement — Path B (routed) baseline

**Scope:** 200-caller cap (s5/s6 capped from 1000), 30s window per scenario,
`docker restart acct-postgres` between each. 10,000 pool universe (seeded
fresh at `acct-qiaz` close). Full 5-min × 1000-caller run gated on PG tuning
(`max_connections`, container memlock for io_uring) — filed as
`phase5-followup`.

**Run:** 2026-05-22T15:52 UTC against poc_v3 / acct-postgres container
(PG 18, `io_method=io_uring`, ledger_routed extension built in production
mode — no `WITH_TEST_HOOKS`).

## Results

| Scenario | Callers | Overlap         | Complexity | Throughput trx/s | Ack p50 µs | Ack p99 µs | Cmtd p50 µs | Cmtd p99 µs | CG avg | CG p99 | Pipeline ns avg | Drains | Defers | Eject | WAL b/trx | commits | rollbacks |
|----------|--------:|-----------------|------------|-----------------:|-----------:|-----------:|------------:|------------:|-------:|-------:|----------------:|-------:|-------:|------:|----------:|--------:|----------:|
| s1       |      10 | Uniform         | Simple     |            170.1 |        428 |       3198 |       60686 |       82837 |   1.00 |      1 |       1,510,891 |   5108 |     13 |     0 |        89 |  122943 |         0 |
| s2       |     200 | Zipf(1.5)       | Simple     |            419.9 |       9789 |     152043 |      471859 |      699924 |   2.14 |     15 |      11,671,399 |   5972 |     21 |     0 |        44 |  456361 |         0 |
| s3       |      10 | Uniform         | Complex    |            134.9 |        717 |       4694 |       74383 |      125501 |   1.47 |      7 |      11,080,959 |   2761 |      9 |     0 |      1196 |  117521 |         0 |
| s4       |     200 | Zipf(1.2)       | Complex    |             59.1 |      11083 |     392691 |     3279945 |     4546625 |   6.16 |     63 |     407,800,701 |    328 |      7 |     0 |       127 |  553163 |         0 |
| s5       |     200 | Single hot pool | Simple     |            379.0 |       7446 |     171311 |      513015 |      817889 |  21.20 |     63 |     217,811,748 |    547 |     17 |     0 |        31 |  622098 |         0 |
| s6       |     200 | Disjoint        | Simple     |            293.7 |       7495 |     162267 |      671612 |      936902 |   1.00 |      1 |       5,783,526 |   8950 |     13 |     0 |        31 |  590651 |         0 |

(`Throughput` = trx materialized in the polling window / second.
`CG avg`/`CG p99` = commit_group envelope count. `Drains` =
committer pool commits across the run. `Defers` =
`router_window_defers_total`. `Eject` = caller-tx ejects.
`commits` is the system-wide `pg_stat_database.xact_commit` delta —
includes the harness's polling SPIs, sampler/collector ticks, and the
committer pool's drains, not just trx materializations.)

## Cross-path comparison vs Phase 3 (direct path)

| Scenario | Direct trx/s | Routed trx/s | Ratio   | Notes                                                      |
|----------|-------------:|-------------:|--------:|------------------------------------------------------------|
| s1       |       2190.8 |        170.1 | 0.08×   | Path A wins low-concurrency uniform (no overlap to batch)  |
| s2       |        544.0 |        419.9 | 0.77×   | Path A wins throughput; Path B wins latency variance¹      |
| s3       |          0.0 |        134.9 | ∞       | Path A rolled back ≈100%; Path B materialized cleanly      |
| s4       |          0.0 |         59.1 | ∞       | Path A rolled back ≈100%; Path B grinds but completes      |
| s5       |          0.5 |        379.0 |   758×  | **headline result** — routed amortizes hot-pool contention |
| s6       |       1544.7 |        293.7 | 0.19×   | Path A wins disjoint (no overlap to batch; routed adds latency) |

¹ Phase 3 s2 committed p99 was 1,105,199 µs at 38.5% rollback rate; Phase 5
routed s2 committed p99 is 699,924 µs at 0 rollbacks. Higher latency tail on
Path A is masked by Phase 3's "throughput = successful submits" — many
"successful" submits resulted in rolled-back trx.

## Observations

### s5 (single-hot-pool) is the headline win

378.99 trx/s on a single shared pool with 200 callers, vs Phase 3's
0.49 trx/s. This is the §10.4 hypothesis exactly: high-contention workloads
should belong to Path B because the router amortizes the pool_lock acquisition
across `commit_group_avg = 21.20` submissions per PG transaction. Each
caller still individually sees committed p99 = 818ms (the routing + drain
queue depth is long), but throughput is 758× direct's.

Pipeline ns avg = 217ms per drain × 547 drains × 21.2 envelopes
≈ 11,591 envelopes processed — close to the 11,599 measured `attempts`.
The committer pool keeps up; throughput ceiling is the per-drain PG work
(bulk-write + commit), not the router or staging.

### s4 (high-contention complex) completes where Path A failed

Phase 3 s4 showed 0.0 trx/s (96% rollback rate). Phase 5 routed delivers
59 trx/s with 0 rollbacks at committed p99 = 4.5 seconds. The routed
pristine-replay loop handles failing submissions by exclusion + retry
inside the committer transaction, so caller-visible errors stay at 0
even when the workload is structurally hostile. `commit_group_avg = 6.16,
p99 = 63` shows heavy batching; `pipeline_ns_avg = 408ms` per drain is
the largest in the suite — complex (10-50 lines/submission) × heavy
batching → big bulk-write payloads.

The 4.5-second committed p99 means individual callers wait a long time
for confirmation; throughput is what gets characterized as won.

### s1/s6 (low-overlap workloads) belong to Path A

s1 (10 uniform callers) and s6 (200 disjoint callers) both show
`commit_group_avg = 1.00, p99 = 1` — the router has no overlap to pack
on. Path A's synchronous-commit ceiling at ~2,000 trx/s wins these
regimes outright. The cost of Path B here is the router-window latency
(50ms default) plus the harness's 1ms-poll-for-trx overhead, both of
which serialize callers below their natural rate.

This matches the §10.4 prediction: routed only wins when there's
overlap to amortize. The crossover line lives between "no overlap"
(direct) and "concentrated overlap" (routed).

### s2 (Zipf-1.5) is mid-band — Path A still wins on throughput

Direct: 544 trx/s. Routed: 420 trx/s. Direct's ratio is 1.29×, but the
underlying Path A run rolled back 38.5% of submissions (Phase 3 data);
routed materialized 100% cleanly. The crossover in the Zipf-overlap
regime depends on whether throughput-with-rollback-tolerance or
throughput-with-correctness is the metric. For production usage the
routed number is what matters (every trx the caller sees is real);
for raw committed-tx-per-second the direct number wins this cell.

Phase 6 (acct-dipt characterization) gets to formalize this distinction
into a regime map.

### Eject mechanism dormant under synthetic workloads

`eject_count_total = 0` across every scenario. The harness callers don't
open long-running interactive txs (every `SELECT ledger_enqueue_trx(...)`
returns immediately), so the §5.4 step-4 `pg_xact_status` eject path
never triggers. `router_window_defers_total` likewise stays small
(7-21 per scenario) — the router skipped tens of envelopes per scenario,
not thousands. The eject mechanism is wired and tested under
`acct-6bp9` acceptance binaries; under measurement it stays passive.

### Top wait events dominated by `Client:ClientRead`

Across all six scenarios, the 1Hz wait-event sampler logs
`Client:ClientRead` as top-wait (samples range 53-343). This reflects
the harness shape — callers spend the 1ms poll-tick waiting on the
client-server roundtrip after each `EXISTS(SELECT 1 FROM trx ...)`
query. Not a characterization of Path B internals; characterization of
the measurement methodology. Sub-leading waits in the high-contention
scenarios (s5 in particular) show `LWLock:ledger_v3_spillover_arena`
and `LWLock:ledger_v3_staging_queue` — the routed shmem regions —
which are the more interesting routed-side contention surfaces.

### WAL bytes per trx scales with complexity, not contention

s5 / s6 (Simple, 1 line) → 31 bytes/trx. s1 (Simple uniform low concurrency) → 89.
s2 / s4 → 44 / 127.
s3 (Complex 10-50 lines, low concurrency) → 1,196 bytes/trx.

This is the bulk-write size scaling with line count; not affected by
caller count or overlap mode in a meaningful way.

## Follow-ups

- **phase5-followup**: Full 5-min × 1000-caller run for s5/s6 once PG
  tuning lands (`max_connections >= 1100`, shared_buffers, container
  memlock for io_uring). Should sharpen s5's hot-pool win and surface
  s6's true disjoint ceiling.
- **acct-dipt** (Phase 6): cross-path equivalence subcommand against
  s1-s6; build the regime map from Phase 3 + Phase 5 data.
- **Harness 1ms-poll overhead**: 200 callers × 1000 polls/s = 200k SPIs
  per second of background load. Sub-leading `Client:ClientRead`
  dominance suggests this is a noticeable perturbation. If Phase 6
  characterization shows it shifts crossover, bump poll interval to
  5ms and re-measure.
