Two different disciplines. Correctness needs an oracle and adversarial scheduling; perf needs a real baseline and open-loop load. The current plan (§9–§11) has neither of the load-bearing pieces.

## Correctness

**1. Build a sequential reference model first.** A pure-Rust in-memory oracle (~300 lines): `HashMap<PoolId, ModelPool>` implementing §3 semantics directly, using `num-rational` for exact arithmetic alongside the BIGINT path. This is the single highest-leverage artifact — everything else checks against it. Two comparison modes:

- **Exact mode** (direct flavor, single-threaded): drive the same operation sequence through the model and through `ledger_submit_trx_c`; every field must match byte-for-byte — aggregate qty, value_sum, unit_cost, trx_line contents, posting amounts, error-for-error (including *which* error).
- **Envelope mode** (routed/concurrent): exact equality is undefined per §14.2, so the model instead computes the *set of legal outcomes*. Concretely: given the multiset of submissions, check the actual result is explainable by *some* serialization — final qty ∈ {reachable qtys under any drop-and-continue ordering}, receipts-only pools byte-equal to `banker_div(ΣQC, ΣQ)`, every recorded trx present in the input exactly once. For small N you can enumerate orderings; for large N check the conservation invariants below plus per-pool serializability of the fold.

**2. Conservation invariants as a post-condition sweep, run after every test and every bench run.** SQL, cheap, catches whole classes of bugs no unit test targets:

```sql
-- per pool: aggregate reconciles to the trx_line stream
SELECT pool_id FROM pool_state ps WHERE layer_id = 0
  AND ps.qty <> (SELECT COALESCE(SUM(qty),0) FROM trx_line WHERE pool_id = ps.pool_id);
-- value_sum == net posted amounts (the §3.1 GL-reconcilable claim, tested not asserted)
-- every trx has ≥1 trx_line; every trx_line has expected posting_lines
-- Σ debits == Σ credits per posting_event across the run
-- no (trx_type, source_id) duplicates; every committed harness submission has exactly one trx XOR a recorded drop reason
```

That last one requires the harness to keep its own durable log of what it submitted — which you need anyway (finding 6 from the review: the caller-side outbox). The invariant "trx exists iff submission completed" is currently unverifiable because nothing records intent.

**3. Property-based testing at the ledger-core boundary.** `proptest` over operation sequences per method: arbitrary interleavings of receipts/depletions with adversarial values — qty=1, qty near i64 bounds, unit_cost=0, costs straddling the rounding cliff, value_sum near i64 on the accumulation path. Properties: model equivalence, monotone value_sum for running_avg receipts, the receipts-only permutation invariance (already tested — good), and *the negative-value_sum reachability for standard basis* (this property test would have found review finding 1 mechanically: generate std > actuals, watch the CHECK fire).

**4. Concurrency: control the schedule, don't pray for it.** Stress tests that "run 1000 callers and check the end state" find races at whatever rate the OS scheduler feels like. Three layers:

- **`loom`** for the shmem core — staging/committer queue CAS transitions, arena alloc/free, identity registry generation protocol. Model each state machine as a loom test with 2–3 threads; loom exhaustively explores interleavings including the weak-memory reorderings your `Ordering::` choices permit. This is the only way to actually verify the AUDIT's "memory ordering is sound" claim rather than assert it.
- **`stateright` (or TLA+)** for the cross-process protocols loom can't reach: router/committer/recovery interaction, claim–die–reclaim, generation-vs-pid-recycling, the eject/backpressure interlock. Model the protocol, not the code; check liveness (every committed-caller submission eventually terminal) and safety (no submission processed twice, no in_flight orphaned forever). The pid-recycling liveness hole and the backpressure livelock are exactly the shape of bug a model checker finds in minutes.
- **Fault injection in integration tests**, made deterministic via the existing `test_hooks` feature: pause a committer between pre-flight dedup and INSERT (forces the 23505 re-drive path on demand), kill between COMMIT and cleanup CAS (forces reclaim-of-completed-work), kill router mid-`processing`. Every branch in §6.5/§6.8 should have a test that *forces* it, not one that hopes to hit it. Add `kill -9` postmaster loops: run workload, SIGKILL at random offset, restart, run invariant sweep, repeat ×500 overnight. PG's crash recovery is being trusted here; verify the composition.

**5. Targeted tests for the semantic edges:** savepoint-rollback around enqueue (phantom-trx check — pins the xid-granularity decision), duplicate (trx_type, source_id) with *different* payloads (documents first-wins or catches it), same-key submissions forced into disjoint-pool commit_groups (exercises the cross-group race deliberately), a receipt+depletion pair straddling a forced chunk boundary (pins the spurious-drop behavior as documented-or-fixed), basis flip mid-stream, standard revision mid-stream.

**6. Recalc-preview as oracle, even with recalc deferred.** An offline strict-FIFO replay over the trx_line dump (throwaway, doesn't need production quality) closes the loop: it verifies the trx_line stream is *sufficient* to reconstruct authoritative state — which is Path C's entire premise and currently untested. If replay can't produce a well-defined answer from what the hot path recorded (backdated receipts, chunk reordering), you've falsified the architecture cheaply. It also gives you the variance-magnitude distribution for free.

## Performance

**7. Measure lock-hold directly, not by proxy.** Ack latency flat-vs-depth is consistent with constant lock-hold but doesn't measure it. Instrument the critical section itself: timestamp immediately after the last pool_lock `FOR UPDATE` returns and immediately before COMMIT-return (direct) / before the write-phase subtx commit (routed), accumulate into an in-extension HDR histogram exposed via a SQL function. Cross-check externally with `log_lock_waits` + `pg_wait_sampling` (wait-event profile tells you whether time goes to LWLocks, WALWrite, or the row lock — the "constant" claim should hold in the wait-event breakdown, not just the total).

**8. Open-loop load generation, or the latency numbers are fiction.** The harness is closed-loop: N callers each submit-then-wait. Closed-loop load suffers coordinated omission — when the system stalls, callers stop generating, and the stall never appears in the latency distribution. For every latency claim (and the routed-vs-direct crossover *is* a latency claim at the margins), drive an open-loop Poisson arrival process at fixed target rates and record per-submission latency from intended-send-time. Report full distributions (p50/p99/p999 with HDR), never means. Closed-loop is fine for peak-throughput measurement only.

**9. Establish the real baselines.** Without them every number is self-referential:

- **Strict-mode Path A on the same box** — the linear-in-depth curve is the entire motivation; measuring Path C flat without measuring strict linear proves nothing about the *decision*. This is one build of the v3 workspace.
- **Raw ceiling**: plain `INSERT INTO trx_line` batch throughput on the same hardware — bounds how much of Path C's cost is the ledger vs. the substrate.
- **Ablations**: routed with `batch_size_max ∈ {1, 50, 200, 1000, ∞}`, `pack_disjoint` on/off, advisory-lock variant vs pool_lock row (a day of work; directly answers review finding 7 with data), `SetLatch`-on-enqueue vs 50ms tick.

**10. Kill the known confounds before trusting the crossover map.** pgbouncer pool size is a hidden variable — sweep it (25/50/100/200) for S5/S7/S8 and show the crossover is stable, or report it as a function of pool size. Pin CPUs, isolate WAL onto its own device or at minimum report fsync latency of the device, fix `shared_buffers`/`checkpoint_timeout` and *report them*, disable autovacuum during short runs but see #11.

**11. Soak runs, because the interesting failures are temporal.** 30-second runs never cross a checkpoint, never accumulate clog, never trigger autovacuum, never reveal arena fragmentation (an arena leak was already found once — by code reading, not by test). Run 1–4 h sustained at ~70% of measured saturation and plot throughput/p99 over time against `pg_stat_bgwriter`, table/index bloat on pool_lock and pool_state, WAL volume, and — given the xid-burn issue — `age(datfrozenxid)` slope. A flat 30s number with a sawtooth 2h profile is a different architecture verdict.

**12. Statistics discipline.** ≥5 runs per cell with fresh seeds, randomized execution order across cells (thermal/cache drift), report median + bootstrap CI, compare cells with Mann-Whitney rather than eyeballing. Declare the steady-state window mechanically (discard until a rolling CoV threshold), not by trimming the first N seconds by feel. Any crossover claim ("routed overtakes direct at concurrency X") gets an uncertainty band, because that X is the deliverable the production decision consumes.

**13. Saturation methodology.** Ramp offered load until goodput plateaus/degrades; report **throughput at latency SLO** (e.g., max sustainable rate with p99 < 10ms) as the headline, with peak throughput as a secondary. Peak-throughput-only comparisons systematically flatter routed (it trades latency for batching) — the current framing bakes that bias in.

Priority order if effort-constrained: reference model + invariant sweep (#1–2), loom on the shmem core (#4), open-loop generator (#8), strict-mode baseline (#9), soak run (#11). Those five change what the PoC can honestly claim; the rest sharpen it.
