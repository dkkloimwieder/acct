# FIFO arena correctness audit — 2026-05-14

acct-zm69 Phase 1. Walk each candidate (B / E / F + a proposed new
design G) against the seven correctness criteria and the six
untested concurrency shapes. Per the methodology rule the issue is
explicit about — `audit-review-work-that-calls-itself-comprehensive-or` —
each criterion is evaluated independently per candidate; no skipping
to matrix summary.

## A2 — the structural failure

For context. The two load-bearing call sites:

- `fifo.rs:1800` — `consume_from_head(&mut shadow.ring, p.qty)` runs
  against a per-backend, per-call shadow of the ring. The shadow is
  cloned from shmem under SHARED at `fifo_acquire_shadow`.
- `fifo.rs:1809-1833` — slices the consume returned are pushed
  inline into `dp_*` arrays (depletion rows) and into the shadow's
  `pending_drain` ring. **These writes commit with the user txn.**
- `fifo.rs:1175-1179` — at `xact_commit` replay,
  `LayerOp::Consume { qty }` calls `consume_from_head` against the
  real ring and **discards the result**. The depletions were already
  written from the shadow snapshot's view.

R-MB6's failure mode walked through:

1. Backend A and B both apply `fifo_issue qty=100` against a thin
   layer L with `qty_received = 100`.
2. Both call `fifo_acquire_shadow` and clone the ring under SHARED.
   Both shadows see L at qty=100.
3. Both call `consume_from_head` against their own shadow. Both
   shadows transition L from 100 → 0. Both produce slices
   `{ layer_id: L, qty_consumed: 100 }`.
4. Both push depletion rows into `dp_*` and pending_drain entries
   into their shadows. Phase 9d INSERTs both rows; SUM(depletions)
   = 200 > 100 = qty_received.
5. At commit, A's replay drives the real ring from 100 → 0. B's
   replay sees an already-empty real ring; `consume_from_head`
   returns `shortage=100`, the result is discarded.
6. Pending_drain replay tries to push two `qty_consumed=100`
   entries; bgworker drain hits `CHECK qty_remaining >= 0` and
   silently rolls back the drain batch. `cost_layers.qty_remaining`
   never reflects the truth that L was double-consumed.
7. Two COGS postings exist in `posting_lines` against an inventory
   movement that physically could have only happened once.

The depletion rows are committed against a ring snapshot that
**could not have been current at commit time**. That's the
structural failure: A2's apply-time SPI INSERTs against shadow-view
slice identities, plus optimistic commit-time replay that discards
contradicting results.

## The seven correctness criteria (verbatim from acct-zm69)

For every committed posting_line:
1. `SUM(qty_consumed for layer L) <= cost_layers.qty_received for L`
   holds at commit time, not as a recon-detected drift.
2. `cost_layers.qty_remaining` is true residual; bgworker drain
   never silently drops entries.
3. Account balances reflect actual movement; no committed
   posting_line is structurally double-attributed.

For every aborted txn:
4. No durable state change persists (txn semantics).
5. No shmem state change persists past abort visible to other
   backends.
6. Cell state remains correct: subsequent reads see pre-abort truth
   without waiting on a recon-tick repair.

For every savepoint:
7. `ROLLBACK TO` restores the in-shmem cell state to the savepoint's
   logical view, observable immediately.

## The six untested shapes (carried over from acct-a3rj)

- S1 — 3+ backends concurrent on the same thin layer.
- S2 — concurrent over-consume across multi-layer pools.
- S3 — receipt commit racing concurrent issue lazy-seed.
- S4 — ring overflow flip racing concurrent issues.
- S5 — identical `idempotency_key` from concurrent backends.
- S6 — `fifo_arena_recon` (SHARED walker) concurrent with apply
  (EXCLUSIVE at commit).

## Candidate B — OCC PreCommit validation

Sketch: each backend snapshots a per-cell `validation_seq` at the
moment it acquires the shadow. At PreCommit, in a sorted EXCL pass,
revalidate that no other backend has committed a state change
against the cell since snapshot (`current_seq == snapshot_seq`). On
mismatch, raise SQLSTATE 40001 → SQL aborts the whole txn (rolling
back depletions, posting_lines, and cost_layers writes). On match,
apply the staged ops to the real ring, bump `validation_seq`, stamp
`last_seq` for the bgworker.

Where SPI writes go: still inline in user-txn time, **same as A2**.
Validation-failure-as-40001 raised from PreCommit causes SQL to
roll back those SPI writes via standard txn semantics.

### Per-criterion walk

- **C1 (no over-consume)** — PASS. Backends A and B both snapshot
  with `validation_seq = N`. First to PreCommit (say A) wins,
  applies staged ops, bumps seq to N+1. B's PreCommit revalidation
  sees seq != N, raises 40001. B's txn aborts; B's depletion rows
  roll back. Final state: only A's depletion committed against L,
  SUM <= qty_received holds by construction.
- **C2 (qty_remaining true)** — PASS conditional on staging
  pending_drain entries under the same validation barrier.
  Validation-failed txns release their staged pending_drain entries
  via shadow discard at xact_abort. No orphan entries reach the
  bgworker → no silent CHECK rollback in the drain.
- **C3 (no double-attribute)** — PASS. Equivalent to C1: only the
  PreCommit-winning backend's depletion rows commit.
- **C4 (no durable persist on abort)** — PASS. Standard SQL txn
  rollback handles this.
- **C5 (no shmem state persists past abort visible to others)** —
  PASS. The shadow is never installed into the real ring on an
  aborted txn. `fifo_xact_abort` already clears the pending stack;
  PreCommit-validation-failure is just another path to abort.
- **C6 (subsequent reads see pre-abort truth without recon repair)** —
  PASS. The real ring was never mutated; subsequent reads see the
  pre-validation state.
- **C7 (savepoint ROLLBACK TO restores logical view)** — UNCLEAR.
  The proposal handles top-level abort cleanly. Subxact handling
  requires per-frame snapshot of validation_seq AND careful
  treatment of sub-commit→parent merging. When a child subxact
  commits into the parent, the parent's validation_seq snapshot
  must inherit the child's staged ops without re-snapshotting (or
  the parent's PreCommit would fail to see its own staged work).
  Open design question; not a structural failure.

### Untested-shape coverage

- **S1 (3+ backends thin layer)** — PASS by construction. Only one
  backend's PreCommit wins; others raise 40001. Retry pressure
  scales linearly with backend count, but correctness holds.
- **S2 (multi-layer concurrent over-consume)** — PASS. Each cell's
  validation_seq independently tracks contention; the structural
  argument is per-cell.
- **S3 (receipt commit racing issue lazy-seed)** — PASS conditional
  on receipt-commit also bumping validation_seq. Lazy-seed's
  snapshot under SHARED captures pre-commit version; receipt's
  PreCommit bumps version; issue's PreCommit sees drift → 40001.
- **S4 (ring overflow flip racing issues)** — PASS conditional on
  `OverflowActivate` ops bumping validation_seq at commit replay.
- **S5 (identical idempotency_key)** — PASS via SQL unique
  constraint on `posting_lines.idempotency_key` (already in place
  pre-A2). Second backend's INSERT raises 23505; FIFO arena need
  not handle it.
- **S6 (recon walker concurrent with apply)** — UNCLEAR. Recon is
  SHARED-only and currently does multi-field reads non-atomically.
  Adding `validation_seq` doesn't help recon read consistency;
  recon would still need a seqlock-style retry pattern to read a
  consistent ring snapshot. Sibling concern, not a B-specific
  failure.

### Verdict — B
**PASS on C1-C6; UNCLEAR on C7 (subxact design)**. Closes the
over-consume gap structurally. Multi-week build because every code
path that touches the ring must bump validation_seq, and every code
path that snapshots the ring must capture validation_seq. The
biggest design risk is C7 — subxact validation_seq accounting needs
to handle child-into-parent merging without double-counting.

---

## Candidate E — In-place mutation + needs_repair

Sketch: drop the shadow. Each apply holds EXCL on the cell across
the apply window (acquire-mutate-release per envelope, or per
batch). Mutate the real ring inline. On txn abort, flag touched
cells with `needs_repair`. Recon-tick walks `needs_repair` cells,
EXCL-wipes the ring, reseeds from durable `cost_layers`.

### Per-criterion walk

- **C1 (no over-consume)** — PASS at apply time. EXCL serialization
  per cell means B sees A's mutation before it consumes; second
  backend's `consume_from_head` returns `shortage > 0` → error.
  Over-consume is prevented at the apply phase.
- **C2 (qty_remaining true)** — PASS conditional on drain semantics
  staying in sync. Pending_drain writes are inline under EXCL; no
  silent drops.
- **C3 (no double-attribute)** — PASS. Only the EXCL-winning
  backend writes the depletion.
- **C4 (no durable persist on abort)** — PASS via SQL semantics.
- **C5 (no shmem state persists past abort visible to others)** —
  **FAIL**. This is the structural failure mode. Sequence:
  1. Backend A acquires EXCL, mutates ring (consume 100 from L →
     L.qty = 0), writes depletion row, releases EXCL.
  2. Backend B acquires EXCL, reads L.qty = 0 → shortage error.
     B's txn aborts.
  3. Backend A's txn aborts (separately — say a SQL constraint
     violation later in the txn).
  4. A's `xact_abort` flips `needs_repair = true` on the cell.
     The ring still shows L.qty = 0. The depletion row is rolled
     back by SQL.
  5. Until the recon-tick processes `needs_repair`, any backend
     reading the cell sees L.qty = 0 — which is **wrong**
     (durable cost_layers still shows L.qty_remaining = 100).
  6. The window between A's abort and the recon-repair is
     observable corruption.

  The proposal can shrink this window (e.g., synchronous repair on
  next EXCL acquire) but cannot eliminate it without serializing
  on a global lock. Even a synchronous-repair-on-next-acquire
  design has a window between A's abort and the next acquire that
  could be filled by a SHARED read for `balance_lookup`.
- **C6 (subsequent reads see pre-abort truth without recon repair)** —
  **FAIL**. Same root cause as C5. Reads through `balance_lookup`
  or `ledger_shmem_recon` would observe the post-mutation pre-
  repair state.
- **C7 (savepoint ROLLBACK TO)** — **FAIL**. Subxact abort would
  need to revert in-place mutations made by the subxact's apply.
  E's design has no record of what to revert (the mutation already
  happened to the real ring). The `needs_repair` mechanism is
  whole-cell-wide; a subxact ROLLBACK TO would have to wipe and
  reseed the cell, losing the parent txn's prior in-place work.
  The parent's subsequent reads see the reseeded (durable) state,
  which lacks the parent's pre-subxact work — wrong direction.

### Untested-shape coverage

- **S1 (3+ backends thin layer)** — Apply-time correct (EXCL
  serializes); abort-window remains broken per C5.
- **S2 (multi-layer)** — Same.
- **S3 (receipt commit racing lazy-seed)** — UNCLEAR. Both paths
  hold EXCL during the seed/receipt; serialization handles it. But
  abort-window still applies to either path's failure.
- **S4 (overflow flip)** — Same as S2.
- **S5 (idempotency_key)** — Handled by SQL unique.
- **S6 (recon walker)** — Recon would observe `needs_repair` cells
  and either skip them (drift reported) or wait for repair.
  Manageable but adds recon complexity.

### Verdict — E
**Apply-time PASS; abort-window FAIL on C5/C6/C7**. E correctly
handles the concurrent-apply over-consume case via EXCL
serialization, but introduces a NEW correctness gap: the
abort-to-repair window is an observable-corruption window that
violates the issue's "no recon-tick repair" requirement.
E is not correct by construction; it trades A2's commit-time
over-consume for an abort-time observable corruption window.

---

## Candidate F — Undo-log

Sketch: same shape as A2 (per-backend shadow + commit-time replay)
but record explicit undo ops alongside the apply mutations. On txn
abort, replay undo ops to restore the shadow state to its pre-apply
shape. The real ring is still only mutated at commit.

### Per-criterion walk

- **C1 (no over-consume)** — **FAIL**. The undo log captures
  per-backend state; it does NOT mediate between backends. Both
  backends still snapshot the ring under SHARED, both produce
  depletions against their shadow's view of L. Commit-time replay
  still has the same problem A2 has — the second backend's
  Consume op replays against an already-emptied real ring, the
  result is discarded, the depletion row is already committed.
  Undo solves intra-backend abort cleanliness; it does not solve
  inter-backend optimistic conflict.
- **C2-C7** — Inherit A2's behavior. C5 and C6 are slightly better
  than A2 (undo replay restores the shadow on subxact abort,
  matching pre-subxact view). But the C1 failure subsumes
  everything: the structural over-consume is still there.

### Untested-shape coverage

- All six shapes inherit A2's behavior. F adds no new mechanism
  for inter-backend coordination. The S1-S4 over-consume cases
  remain unfixed.

### Verdict — F
**FAIL on C1**. F is an A2 variant with cleaner abort semantics
but no inter-backend mediation. It would be a strict improvement
on A2's commit-time-discard-result pattern (because intra-backend
aborts are explicit rather than implicit), but it does not address
the load-bearing correctness gap. Reject.

---

## Candidate G — OCC + commit-time apply (proposed new design)

Stronger version of B. Rather than mutating the real ring lazily
on commit-replay using the snapshot's view, G mutates the real
ring at PreCommit by **re-executing** the staged ops against the
post-validation real ring under EXCL. Depletion writes are deferred
to PreCommit too — they go to a per-backend SPI buffer at apply
time and only get INSERTed at PreCommit, after revalidation, with
slice identities computed against the post-validation ring.

The shape:

1. **Apply phase (SHARED-only)**: snapshot the ring, capture
   `validation_seq`. Run `consume_from_head` against the snapshot
   purely to compute `total_cost` for the posting_line amount
   field (so the SQL writes that DO happen in user-txn time —
   posting_lines and cost_layers — can use the right amounts).
   Stage ops + provisional depletion candidates in pending_stack.
   Do NOT INSERT cost_layer_depletions yet.
2. **User-txn time SPI**: INSERT posting_lines + cost_layers as
   normal (these are needed for the amount/qty audit trail and
   their idempotency_key surfaces SQL unique violations early).
   Do NOT INSERT cost_layer_depletions.
3. **PreCommit (EXCL per touched cell, sorted)**: revalidate
   `validation_seq` per cell. On mismatch, raise 40001 → txn
   aborts.  On match, replay the staged ops against the real ring.
   Re-derive depletion slice identities from the post-replay
   ring. INSERT cost_layer_depletions + stage pending_drain in
   one final SPI batch.

PreCommit's SPI is safe — PreCommit runs in transaction-active
state with SPI permitted (this is the same window as A2's commit-
time replay; A2 already does work here, just non-SPI work).

### Per-criterion walk

- **C1 (no over-consume)** — PASS by construction. The depletion
  rows are written from the post-revalidation ring state. Any
  contradictory consume returns a shortage that surfaces as an
  error from PreCommit (which aborts the txn).
- **C2 (qty_remaining true)** — PASS. pending_drain entries are
  staged in PreCommit alongside the depletion writes; orphan
  drain entries cannot exist because aborted txns never reach
  PreCommit's commit branch.
- **C3 (no double-attribute)** — PASS by construction. Same logic
  as C1; the depletion's `(layer_id, qty_consumed)` agrees with
  the real ring's post-replay state.
- **C4 (no durable persist on abort)** — PASS via SQL semantics.
  The user-txn-time INSERTs to posting_lines and cost_layers roll
  back; cost_layer_depletions was never inserted in the first
  place.
- **C5 (no shmem persist past abort visible to others)** — PASS.
  Aborted txns never reach the real-ring-mutation step.
- **C6 (subsequent reads see pre-abort truth without recon)** —
  PASS. The real ring is never mutated until PreCommit's
  validation-success branch.
- **C7 (savepoint ROLLBACK TO)** — PASS conditional on the same
  subxact accounting B needs. Per-frame validation_seq snapshots
  + sub-commit-merges-into-parent + sub-abort-pops-and-discards
  give correct subxact semantics, because nothing was ever
  applied to the real ring during the subxact (deferring all
  ring mutation to outermost PreCommit).

### Untested-shape coverage

- **S1 (3+ backends thin layer)** — PASS. PreCommit serialization
  + validation_seq → only one backend's PreCommit-validation
  passes; others raise 40001.
- **S2 (multi-layer)** — PASS. Per-cell validation_seq;
  multi-cell PreCommit just iterates cells in sorted order.
- **S3 (receipt commit racing lazy-seed)** — PASS. Receipt's
  PreCommit bumps validation_seq when it pushes new layers;
  concurrent issue's PreCommit revalidation sees drift, aborts,
  client retries with fresh snapshot.
- **S4 (overflow flip racing issues)** — PASS. Flip bumps
  validation_seq.
- **S5 (idempotency_key)** — PASS via SQL unique on
  posting_lines (which is INSERTed pre-PreCommit, so the unique
  violation happens before PreCommit even runs — clean error
  path).
- **S6 (recon walker concurrent with apply)** — UNCLEAR / minor.
  Recon's SHARED walks during apply are fine (apply doesn't touch
  the real ring). Recon's SHARED walks during PreCommit could
  observe mid-replay state. Solvable with seqlock on the cell's
  fields OR by gating recon on a per-cell mutex. Same concern
  applies to B.

### Verdict — G
**PASS on C1-C7 by construction. S1-S5 covered structurally; S6
needs a seqlock-style read pattern, shared with B.**
G is B's design with one additional invariant: depletion writes
happen post-validation, not pre-validation. This eliminates the
"write rollback waste" pattern (where validation-failed txns
INSERT depletion rows that get rolled back by 40001) at the cost
of moving INSERT work into the PreCommit window. The trade-off:
fewer wasted SPI writes under contention; longer PreCommit
window per backend.

---

## Candidate H — Append-only layers + deferred constraint (added post-initial-audit)

Sketch (proposed by external review): layers are immutable. The
`cost_layers` table is INSERT-only. Reversals, adjustments, merges
are new layer rows with signed qty. Consumptions are append-only
rows in a separate table. "Current state" is a derivation:
`SUM(layer qty) - SUM(consumption qty)` per `layer_group_id`. A
`DEFERRABLE INITIALLY DEFERRED` constraint trigger on
`cost_consumptions` runs at commit time, raises `40001` if any
touched layer_group's effective qty went negative.

PG MVCC + the deferred constraint are the entire concurrency
mechanism. The shmem ring becomes a pure read-side cache; the
bgworker drain mechanism (PENDING_DRAIN_CAP, CHECK qty_remaining
>= 0) does not exist.

### Load-bearing assumption

H's correctness against C1 (no over-consume) requires **SERIALIZABLE
isolation**. Under READ COMMITTED, two concurrent deferred-trigger
SELECTs can both run before either commit has flushed and each
returns a stale "effective = 100" reading for a layer they both
intend to drain. Both pass; both commit; invariant violated.

PG only catches this under SERIALIZABLE via SSI's predicate-lock
machinery. The proposal as originally written says "PG MVCC + a
deferred constraint" without naming the isolation level; the
correctness story compresses to **H works iff every txn touching
`cost_consumptions` runs under SERIALIZABLE.**

### Per-criterion walk

- **C1 (no over-consume)** — PASS under SERIALIZABLE; FAIL under
  READ COMMITTED (write skew).
- **C2 (qty_remaining true residual)** — N/A by design.
  `qty_remaining` is derived per query; there is no stored
  residual value to drift.
- **C3 (no double-attribute)** — Same as C1.
- **C4 (no durable persist on abort)** — PASS via SQL semantics.
- **C5 (no shmem state persists past abort visible to others)** —
  PASS. shmem is a read cache; if it briefly returns a stale
  derived value during the abort window, the next read against
  tables corrects it. Canonical state lives in MVCC-respecting
  tables.
- **C6 (subsequent reads see pre-abort truth without recon)** —
  PASS at canonical grain. Cache may briefly lag the abort by one
  invalidation interval; the canonical query against tables is
  always correct.
- **C7 (subxact ROLLBACK TO)** — **PASS, free from PG**. Deferred
  triggers fire only for rows still alive at top-level commit. No
  custom subxact accounting needed. **Strict advantage over G.**

### Untested-shape coverage

- **S1 (3+ backends thin layer)** — PASS under SERIALIZABLE. SSI
  detects the read-write dependency cycle and aborts all but one.
  Retry pressure scales with N.
- **S2 (multi-layer over-consume)** — PASS under SERIALIZABLE.
- **S3 (receipt commit racing lazy-seed)** — **N/A by design**.
  There is no lazy-seed; readers always go to tables. Strict
  advantage.
- **S4 (ring overflow racing)** — **N/A by design**. There is no
  ring. Strict advantage.
- **S5 (identical idempotency_key)** — PASS via SQL unique on
  `posting_lines.idempotency_key`.
- **S6 (recon walker concurrent with apply)** — PASS. Recon reads
  tables under MVCC; cannot observe partial state.

### Risks the proposal understates

- **SERIALIZABLE perf characteristics**. Predicate-lock overhead
  on every trigger SELECT. False-positive aborts: SSI is
  conservative; two backends issuing against *different* layers in
  the same item can be aborted by predicate-match heuristics.
  Bench-uncovered at our contention shapes.
- **Reversal-after-consumption raises 40001 inappropriately**.
  Trigger semantics conflate "concurrent over-consume" (retryable
  serialization conflict) with "retroactive over-adjustment"
  (business error; retry will keep failing). Both surface as
  40001. Application cannot distinguish.
- **Concurrent adjustment vs consumption has commit-order
  non-determinism**. Operator-visible outcome depends on which
  commit reaches WAL first.
- **Aggregate trigger cost grows with consumption history per
  group**. PoC-scale fine; multi-year ERP scale needs archival.
  Mitigation deferred in the proposal.
- **Merge case under consumption pressure**. If L1 has prior
  consumptions and is then closed by `L1_close`, the consumption
  rows still reference L1; trigger walking L1's group sees
  negative effective qty. Either re-attribute consumptions
  (defeats append-only) or special-case closed groups. Proposal
  hand-waves.
- **Migration cost**. ~50% of `fifo.rs` (shadow + replay +
  pending_drain + XactCallback) becomes obsolete. Phase B
  regression net needs reshaping. R-MB1–R-MB6 + R-BG + R-SP +
  R-CR test harnesses reshape to address the new model. Greenfield
  memory says no mixed-mode, so this is a clean replacement —
  but it's a large one.

### Strict advantages over G

- C7 is free from PG semantics; G needs subxact validation_seq
  accounting design.
- S3, S4, S6 are eliminated by removing the ring + lazy-seed,
  not solved by validation discipline.
- ~50% code reduction in `fifo.rs`. shmem becomes a pure cache.
- Audit-trail quality: every receipt / reversal / adjustment is
  a queryable row with `source_kind` + `source_ref`. G inherits
  A2's "depletion rows attribute to a snapshot view" semantics.

### Verdict — H
**PASS on C1–C7 under SERIALIZABLE. Several criteria become N/A
by design rather than satisfied by mechanism. Strict architectural
advantages over G. Load-bearing dependency on SERIALIZABLE
behavior at our contended shapes — UNKNOWN until benched.**

---

## Comparison matrix

|       | C1  | C2  | C3  | C4 | C5  | C6  | C7  | S1  | S2  | S3  | S4  | S5 | S6  | Isolation |
|-------|-----|-----|-----|----|----|----|----|----|----|----|----|----|----|----|
| **B** | ✓   | ✓   | ✓   | ✓  | ✓   | ✓   | ?   | ✓   | ✓   | ✓   | ✓   | ✓  | ?   | RC        |
| **E** | ✓ⁿ  | ✓   | ✓ⁿ  | ✓  | ✗   | ✗   | ✗   | ✓ⁿ  | ✓ⁿ  | ?   | ✓ⁿ  | ✓  | ?   | RC        |
| **F** | ✗   | =A2 | ✗   | ✓  | =A2 | =A2 | =A2 | ✗   | ✗   | ✗   | ✗   | ✓  | =A2 | RC        |
| **G** | ✓   | ✓   | ✓   | ✓  | ✓   | ✓   | ?   | ✓   | ✓   | ✓   | ✓   | ✓  | ?   | RC        |
| **H** | ✓ˢ  | n/a | ✓ˢ  | ✓  | ✓   | ✓ᶜ  | ✓   | ✓ˢ  | ✓ˢ  | n/a | n/a | ✓  | ✓   | **SSL**   |

Legend: ✓ pass by construction; ✓ⁿ pass at apply with abort-window
caveat; ✓ˢ pass under SERIALIZABLE only; ✓ᶜ pass at canonical
(table) grain, cache may briefly lag; ✗ fail; ? unclear / needs
design; n/a the shape does not exist in this design; =A2 inherits
A2's behavior; RC = READ COMMITTED; SSL = SERIALIZABLE.

## Recommendation (revised after H bench, 2026-05-14)

**Prototype H.** The SERIALIZABLE-ceiling probe (zm69.h0,
`poc/batch-ledger/bench/results-h-probe-2026-05-14.md`)
empirically validates the load-bearing assumption + clears the
acceptance bar by a wide margin:

- Realistic mix (50 groups, 20 writers): **1,239 committed/s, 0
  invariant violations, 0.91 retries/commit, p99=57ms**. That's
  33× A2's 37.4 committed/s baseline.
- Pathological contention (1 thin layer, 20 writers): 108.9
  committed/s under SSL with 16.5 retries/commit; still ~3× A2's
  baseline.
- Disjoint ceiling: 4,737 committed/s (single-row-per-txn shape;
  batched-H will close most of the gap to bare-INSERT's 80K).
- **RC contended control empirically exhibits write-skew**: 1
  layer_group ended with negative effective qty after 30s. SSI
  is verified to be load-bearing — confirming the audit's
  analysis.

Update from the audit's initial recommendation: G is no longer
the lean. H clears the bar on every measured criterion, and its
strict architectural advantages over G (free subxact, S3/S4
eliminated by design, ~50% code reduction, superior audit trail)
are real.

### Why this order

G dominates B/E/F on the criteria they all probe (the audit's
original walk). G shares H's pass column on C1–C6; H strictly
beats G on C7 (free from PG vs design risk in G) and eliminates
S3/S4/S6 by design rather than solving them with mechanism.

The unknown that gates the choice between G and H is **whether
SERIALIZABLE's SSI behaves acceptably under our contended
workloads**. The acct-zm69 acceptance bar is ≥33.7 committed/s @
5% rollback fan-out, 5K pools, 60s × 20w — A2's measured baseline
is 37.4. If H's SERIALIZABLE configuration sustains that throughput
at acceptable retry-rate, H is the strictly better path on
architectural grounds (≈50% code reduction, free subxact, audit
trail superior). If SSI false-positive aborts crater throughput
under our contention shape, G is the answer.

The bench harness is small enough that running it BEFORE
prototyping G is the cost-effective ordering: G's prototype is
2-3 weeks; H's SERIALIZABLE probe is half a day to a day. Probe
first.

### Rejected

E is **rejected**. It cannot satisfy C5/C6/C7 without effectively
becoming G (or equivalently, mutating shadow state and deferring
real-ring mutation to commit — which is what G already does).

F is **rejected**. F is a strict improvement on A2 for intra-
backend abort cleanliness but does not address the inter-backend
gap, which is the issue acct-zm69 was filed for.

### Either G or H needs

1. (G only) **Subxact validation_seq accounting design**. The
   mechanics of how child-frame validation_seq snapshots merge
   into parent on `SUBXACT_EVENT_COMMIT_SUB` is the load-bearing
   design open question. H gets this free from PG.
2. (G only) **Seqlock-pattern reads** for `ledger_shmem_recon`
   and `ledger_balance_lookup`. Shared with `acct-zo4t`. H's
   shmem-is-just-a-cache structure makes this less load-bearing
   (canonical state is in tables) but the cache invalidation
   discipline still has its own correctness story.
3. (Both) Resolution of the **40001 conflation** issue (H) or
   **retry-storm UX** (G). Pure concurrency conflicts retry
   cleanly; business errors masquerading as 40001 do not.
4. (Both) **Phase B regression net** (`cost_layers.qty_received`
   + `fifo_overconsume_check`) gets repurposed as the
   "post-design-must-hold" invariant; under H the column may
   become redundant (qty_received = SUM of positive-layer qty per
   group), under G it stays as-is.

---

## Phase 2 entry — H probe (zm69.h0)

Before committing to G or H, run a SERIALIZABLE-ceiling probe.

### What we need to measure

Two regimes, each as a standalone bench binary in
`poc/batch-ledger/tests/` (separate crate, separate DB —
`acct_poc`):

**Regime 1: disjoint-write ceiling.** 5,000 distinct
`layer_group_id`s pre-seeded with qty=10,000 each. 20 writer
backends, each loop: pick a random layer_group, INSERT 1
consumption row of qty=1, COMMIT under SERIALIZABLE. Measure
throughput + retry rate over 60s. **Hypothesis: approaches the
bare-INSERT-with-FK ceiling** (per the togd memory's
append-only A/B baseline: ~80K tps; with constraint trigger
overhead and SERIALIZABLE predicate-locks, expect 30-60K tps).

**Regime 2: contended-write ceiling (R-MB6 shape).** ONE thin
`layer_group_id` with qty=N. 20 writer backends each loop:
INSERT 1 consumption row of qty=1, COMMIT under SERIALIZABLE.
Backends race; SSI aborts conflicts; clients retry. Measure
committed tps + abort-and-retry rate + retries-per-commit ratio.
**Hypothesis: this is where SSI's contention behavior becomes
visible.** Compare to A2's measured 37.4 committed/s @ 5%
rollback baseline.

**Regime 3: realistic-mix.** 50 `layer_group_id`s with qty=1,000
each. 20 writer backends; each picks random group + random qty
∈ [1, 5]. Mix matches the existing `bench_fifo_rollback_inject`
shape used for A2's baseline. **Hypothesis: this is the
load-bearing comparison.** If H beats or matches A2 here, H is
the architectural winner; if H is 5x worse here, G wins.

### What the bench shape is NOT

- Not a full prototype of H's extension code. The bench is a
  **pure-SQL** probe — no `ledger_extension` involvement, no
  shmem cache, no bgworker. The point is to measure
  SERIALIZABLE + deferred-constraint behavior at our contention
  shapes, isolated from any extension overhead.
- Not a comparison against extension-based A2. The append-only
  baseline (`bench_fifo_inserts_only_ceiling.rs`'s shape) is the
  apples-to-apples comparison: bare INSERTs vs INSERTs +
  constraint trigger + SERIALIZABLE.
- Not idempotent-key tested. Keep idempotency layered on; not
  load-bearing for the probe.

### Schema for the probe

```sql
CREATE TABLE cost_layers_h (
    layer_id BIGSERIAL PRIMARY KEY,
    layer_group_id BIGINT NOT NULL,
    qty BIGINT NOT NULL,
    unit_cost BIGINT NOT NULL,
    born_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    source_kind TEXT NOT NULL
);
CREATE INDEX cost_layers_h_group_idx ON cost_layers_h (layer_group_id);

CREATE TABLE cost_consumptions_h (
    consumption_id BIGSERIAL PRIMARY KEY,
    layer_group_id BIGINT NOT NULL,  -- denorm for trigger speed
    qty BIGINT NOT NULL,
    unit_cost BIGINT NOT NULL,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX cost_consumptions_h_group_idx ON cost_consumptions_h (layer_group_id);

CREATE OR REPLACE FUNCTION check_no_overconsume_h() RETURNS trigger AS $$
DECLARE
    v_effective BIGINT;
BEGIN
    SELECT
      COALESCE((SELECT SUM(qty) FROM cost_layers_h WHERE layer_group_id = NEW.layer_group_id), 0)
      - COALESCE((SELECT SUM(qty) FROM cost_consumptions_h WHERE layer_group_id = NEW.layer_group_id), 0)
      INTO v_effective;
    IF v_effective < 0 THEN
        RAISE EXCEPTION 'over-consumption: layer_group=% effective=%',
            NEW.layer_group_id, v_effective
            USING ERRCODE = '40001';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER check_no_overconsume_h
    AFTER INSERT ON cost_consumptions_h
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE PROCEDURE check_no_overconsume_h();
```

The trigger aggregates per-group rather than per-layer (matches
the "layer_group as conceptual unit" reading of the proposal,
and means a single trigger fire walks the group's sum once).
This is the cheaper of the two designs in the proposal; if it
proves correct + fast, that's the answer.

### Bench runner shape

Modeled on `bench_fifo_rollback_inject.rs`:

```
sqlx PgPool per backend, max_connections=1
SET default_transaction_isolation = 'serializable' on connection
BEGIN; INSERT cost_consumptions_h ...; COMMIT;
Catch 40001 (SerializationFailure); retry up to N times
Count committed / aborted-then-retried / aborted-final
Report committed-tps, retries-per-commit, p99 latency
```

The retry strategy matters. Standard ERP retry is: catch 40001,
re-snapshot, retry the txn with same business logic. Limit
retries to e.g. 10 attempts. Beyond that, surface as application
error.

### Acceptance signal

The bench output answers: "what does SERIALIZABLE + deferred
constraint look like at our contention shape, on our hardware?"

We decide G vs H based on:

1. **Throughput floor**: Regime 3 (realistic) committed-tps
   compared to A2's 37.4. If H ≥ 30, H is in the running
   (within 20% of A2 baseline; acceptable given architectural
   gains). If H < 15, G wins on perf alone.
2. **Retry budget**: retries-per-commit in Regime 3. If
   median ≤ 2, retry storm is acceptable. If ≥ 10, SSI's
   conservatism is biting hard at our shape and we'd need to
   layer optimizations (predicate-narrowing indexes, etc.)
   that complicate H's "simple" pitch.
3. **Disjoint ceiling**: Regime 1 throughput compared to the
   bare INSERT baseline (~80K tps from togd). H's disjoint
   ceiling tells us the trigger + SERIALIZABLE base overhead
   without contention. If it's <10K, the constraint-trigger
   evaluation is the bottleneck and H is worse than G
   regardless of contention shape.
4. **Pathological contention**: Regime 2 with 20 writers on
   one thin layer should produce mostly aborts; what matters is
   that committed throughput stays > 0 and the abort rate
   matches expectation (N-1 / N aborts).

### Open methodology questions for the probe

- **SERIALIZABLE vs READ COMMITTED**: bench both as a control,
  not just SERIALIZABLE. Under READ COMMITTED, Regime 2 should
  produce silently-incorrect commits (the write-skew gap H
  exhibits without SSI). Verifying this characterizes H's
  isolation-level dependence empirically.
- **Trigger granularity**: row-level (proposal as written) vs
  statement-level. For multi-row INSERTs, statement-level fires
  once per statement and might amortize better. Test both if
  Regime 1 throughput is below expectation.
- **Index choice**: with `layer_group_id` index on both tables,
  trigger SUM scans should be O(history length) but
  index-backed. Consider whether a partial index on "not yet
  archived" rows helps Regime 1.
- **History growth simulation**: pre-seed `cost_consumptions_h`
  with N rows per group before starting the bench, to simulate
  multi-year ERP scale. Watch throughput vs history size — does
  H degrade linearly?

### Effort estimate

Half day to a day. Three files:

- `poc/batch-ledger/db/migrations/00XX_h_probe_schema.up.sql` —
  schema + trigger.
- `poc/batch-ledger/tests/bench_h_disjoint.rs` — Regime 1 + 3.
- `poc/batch-ledger/tests/bench_h_contended.rs` — Regime 2.

No new bd sub-issue needed — file under `acct-zm69` as a probe.
Filed as `zm69.h0` for clarity.

## Open questions surfaced by the audit (file as zm69 sub-issues)

1. **validation_seq plumbing**: which operations bump the per-cell
   sequence? Candidates: every commit-time real-ring mutation
   (push_layer, consume_from_head, push_pending_drain,
   overflow_activate, seed completion). The set must be exhaustive
   or S3/S4 leak.
2. **Subxact accounting**: when a subxact commits into parent,
   does the parent's snapshot of validation_seq need to "rebase"
   to incorporate the child's staged ops? If yes, the parent's
   PreCommit revalidation must compare against
   `snapshot_seq + sum(child_subxact_ops_count)`, not just
   `snapshot_seq`.
3. **Seed_ops validation_seq**: lazy-seed reads durable
   cost_layers under SPI in user-txn time. If a concurrent
   backend's PreCommit commits new layers between the seed read
   and PreCommit, the seed snapshot is stale. Either (a)
   re-read durable at PreCommit (extra SPI), (b) raise 40001 on
   seeded-bit drift, or (c) accept layer-id-set staleness as
   harmless (the new layers are at the tail, lazy-seed never
   covered them anyway).
4. **PreCommit SPI safety** for G: confirm that
   `Spi::connect_mut` works inside a `RegisterXactCallback` with
   `XACT_EVENT_PRE_COMMIT`. The M10.A2 work for the WAC arena
   established this is safe but used different SPI shapes
   (UPSERT to rollup table); the FIFO arena's batch INSERTs are
   larger.
5. **Recon-during-apply seqlock pattern**: cell field reads in
   `ledger_shmem_recon` and `ledger_balance_lookup` need
   double-load + retry. Reuse the seqlock helper from
   `acct-zo4t` if it lands first.
6. **Bench delta**: G's PreCommit window holds EXCL longer than
   A2's commit replay (replay vs replay-plus-SPI). Expected
   throughput hit at non-contended workloads is small (the EXCL
   hold is already brief). Under contention, retry rate goes up
   for both G and B; the question is how steeply. Target the
   bench in Phase 4 of the epic.

## Phase 2 entry criteria

Before prototyping G:

1. File sub-issues for tests covering S1–S6 against current A2.
   These become characterizing tests (S1, S2, S3, S4 likely fail
   against A2 — they're under-tested versions of R-MB6's failure
   mode; S5 expected pass via SQL unique; S6 expected pass via
   recon's existing tolerance for drift).
2. Resolve open question #2 (subxact validation_seq accounting)
   with a design sketch. The other open questions can be resolved
   during prototype.
3. Confirm scope alignment with the issue: G replaces A2 in full;
   it is not a layered patch on top of A2. Per the greenfield
   memory `feedback_acct_is_greenfield_no_mixed_mode`, no mixed
   mode.

## Phase 2 scope estimate

Sub-issue breakdown for the prototype phase (file under zm69):

- **zm69.t1** — extend tests/fifo_rollback_correctness with R-MB6
  variants for S1–S4 against current A2 (characterization run).
- **zm69.t2** — recon test for S6 (concurrent walker + apply
  pattern probe).
- **zm69.s1** — validation_seq field on `FifoBucket`; plumb
  bumps in every commit-time mutation site.
- **zm69.s2** — capture `validation_seq` snapshot in
  `CellShadow`; modify `fifo_acquire_shadow` accordingly.
- **zm69.s3** — PreCommit XactCallback hook to do sorted-EXCL
  revalidation + staged-op replay + SPI INSERT for depletions.
- **zm69.s4** — defer cost_layer_depletions INSERT from Phase 9d
  to PreCommit.
- **zm69.s5** — subxact accounting design + tests.
- **zm69.s6** — seqlock pattern on FifoBucket field reads
  (shared with `acct-zo4t` if convergent).
- **zm69.b1** — bench against bench_fifo_rollback_inject; verify
  ≥33.7 committed/s @ 5% rollback or beat A2's 37.4.

Estimate ~2-3 weeks total. Largest risk is zm69.s5 (subxact
accounting); s4 is largest pure-mechanical change.

## Note on the regression net

Phase B's `cost_layers.qty_received` + `fifo_overconsume_check()`
SURVIVE and become the regression net for proving G holds the
invariant. R-MB6 stays as-is; under G it must continue to pass,
but the assertion meaning flips: from "detection fires correctly"
to "detection always reports zero rows on committed state because
the gap cannot occur". The assertion polarity of the
`fifo_overconsume_check` query in R-MB6 will need a follow-up flip
when G ships. Until then, R-MB6 remains valid as the documentation
of the failure mode G is designed to prevent.
