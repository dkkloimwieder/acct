# P4 PAC variant — preset-snapshot WAC

Side-experiment of P4 (acct-4dg2) prompted by user question: "i thought WAC
could be improved by using a preset average and then backfilling?"

Yes — this is the PAC (Periodic Average Cost) pattern, equivalent to acct's
`wac_periodic` semantics. The audit follow-up `acct-9wyl` scopes the
backport of PAC to the acct codebase.

## Design

| | Strict WAC perpetual (P4) | PAC variant (here) |
|---|---|---|
| When is the issue cost computed? | At each envelope (running avg post-prior in-batch activity) | Once at batch start (snapshot of pool's `balance/qty`) |
| Where does the math run? | plpgsql FOR LOOP, `jsonb_set` on running map | Pure SQL CTE chain |
| Drift vs canonical avg | None | Accumulates; reconciled at period close (out of PoC scope) |
| acct equivalent | `wac_perpetual` | `wac_periodic` (with provisional flagging + close-hook recompute) |

The PAC variant adds two envelope kinds (`wac_pac_receipt`, `wac_pac_issue`)
and routes them through a no-FOR-LOOP path. Pre-pass: take a snapshot of
each touched pool's `balance / qty`. Apply: every `wac_pac_issue` prices
at the constant snapshot avg.

Empty-pool check moved out of the CTE (the `1/0` trick was constant-folded
by the planner) into a small plpgsql PERFORM ahead of `RETURN QUERY`.

## Measurement (30s spot, 20 workers × 20 pools, sync_on)

| batch_size | P3 simple | P4 strict WAC | P4 PAC | PAC vs strict | PAC vs P3 simple |
|---|---|---|---|---|---|
| **100**  | 10,720 | 10,476 | **15,315** | **+46%** | **143%** |
| **1000** | 40,610 | 22,867 | **27,993** | **+22%** | 69% |

## Findings

**F1. PAC beats strict WAC by 22-46% across batch sizes.** The gain is
larger at smaller batches because the strict-WAC FOR LOOP overhead is a
bigger share of total cost there.

**F2. PAC at batch=100 exceeds P3 simple's batch=100 throughput** by 43%.
Why? Because P3 simple's CTE chain has its own envelope-resolution work
(idempotency replay JOIN, multi-row INSERT, multi-row UPDATE). PAC does the
same plus a snapshot, but the snapshot is one extra CTE on ~20 rows — cheap.
At batch=100 with PAC, the snapshot cost is amortized well.

**F3. PAC at batch=1000 doesn't reach P3 simple's 40,610.** The 28K
ceiling is bounded by multi-row INSERT and UPDATE cost — same in both.
Strict-WAC's FOR LOOP adds ~22% overhead; PAC removes it but doesn't help
beyond.

**F4. The bigger architectural learning**: at batch=1000, the bottleneck
is NOT the cost-dispatch logic — it's the bulk INSERT/UPDATE machinery.
Optimizing the dispatch saves modest %, not orders of magnitude.

**F5. PAC's correctness drift**: an issue late in the batch should see
in-batch receipts' contribution to avg under strict WAC; under PAC, it
sees the pre-batch snapshot. The drift is small per batch (one batch's
worth of new-receipt-cost-vs-snapshot-cost) and accumulates per pool.
acct's `wac_periodic` reconciles this at period close via the close hook
posting a variance line per pool. PoC omits the close hook; this is fine
because the PoC doesn't model period semantics.

## Implications for acct-qdp5 epic

- **acct-9wyl (PAC dispatch backport) becomes more attractive**: 22-46%
  throughput lift on the acct WAC surface, no correctness risk (close
  hook reconciles), audit-surface simplification (no R4 FOR-UPDATE-before-read
  pattern needed because the cost is pre-resolved).
- **P4 result stands**: strict WAC perpetual already exceeds the ≥5K
  target by 4.6×. PAC is a further optimization, not a required path.
- **P8 (acct backport ceiling estimate) refined**: with PAC for WAC + the
  simple-batch shape for the rest of the dispatcher, acct should land
  closer to 15-25K transfers/sec on simple workloads, vs 10-16K with
  strict WAC. Still 60-100× over today's 253 ops/s.

## Files

- `db/migrations/0010_post_batch_pac.up.sql` — PAC `post_batch` variant.
  Notes: doesn't coexist with strict-WAC in the same call (raises if any
  non-PAC kind appears); for PoC measurement only.
- `tests/post_batch_pac_smoke.rs` — 2 smoke tests.
- `tests/bench_p4pac_wac.rs` — bench harness.
- Results: spot measurements only (30s × batch=100 / batch=1000); not
  the full 5×60s replicate sweep. The architectural point is decisive
  enough from spot data; full sweep can be done as part of the acct-9wyl
  backport when it's claimed.
