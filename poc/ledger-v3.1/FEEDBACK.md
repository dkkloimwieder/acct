Read both files end to end. Overall: this is a strong, unusually honest spec — §14.6, §15, and the §7 risk acknowledgment are the kind of self-disclosure most design docs lack. But I found one issue I'd class as a genuine correctness defect (contradicting §15's "none is a correctness defect" claim), several internal inconsistencies, and a few places where the PoC's evidentiary claims are weaker than the framing suggests.

---

## Critical

### 1. `value_sum >= 0` CHECK (migration 0007) is unsound for external-cost-basis depletions

§3.1 defines depletion as `value_sum -= Q × applied_unit_cost` and asserts value_sum "stays equal to the net of the pool's posting_line amounts (GL-reconcilable)." §3.5 says standard-basis FIFO/LIFO depletions use `applied_unit_cost = standard_cost.unit_cost`. These two facts compose badly with the 0007 CHECK:

- FIFO pool, `provisional_basis='standard'`, std = $2.00. Receive 10 @ actual $1.00 → value_sum = $10. Deplete 6 @ std → value_sum = 10 − 12 = **−$2**. qty check passes (10 ≥ 6), the CHECK fires. A legitimate depletion fails.
- Same shape for plain STD pools across a standard revision: receive 10 @ std $1.00, revise std to $3.00, deplete 5 → value_sum = −$5.

This is not an edge case — it's reachable whenever standard exceeds the actual running average, which is half the purpose of having a standard. The spec can't hold all three of: (a) value_sum = net posted amounts, (b) value_sum ≥ 0 on the aggregate, (c) depletions posted at an externally-sourced cost. One must give. Either the CHECK gets scoped to running_avg-basis pools (hard — it's a table constraint, basis is a column on `pool`), or value_sum decouples from posted amounts for external bases (breaking the GL-reconcilable claim and the derived-average model), or the negative-book-value state is admitted (matching what the GL itself does in this scenario).

Severity amplifier in routed flavor: the CHECK fires at **write time** (bulk UPSERT), not at plan time, so drop-and-continue can't drop the offender. 23514 isn't in `classify_phase_error`'s retryable set → the whole commit_group **poisons**, dead-lettering up to 200 innocent submissions on one legitimate standard-basis depletion. In direct flavor the caller gets a baffling check-violation instead of a domain error.

Related: once standard-basis depletions subtract at std, the aggregate unit_cost = `banker_div(value_sum, qty)` is no longer "the running average" — it accumulates the cumulative std-vs-actual variance into the remaining quantity. §14.1's claim that it "remains useful for analytical queries" is false for standard-basis pools (in my example above: value_sum=$0, qty=4, "average" = $0.00 after the first depletion). This also poisons a later basis switch standard→running_avg, which §14.3 says operators may do "at any time."

### 2. Split-chunk reordering can silently drop a *correctly ordered* depletion — and routed flavor has no error channel

§14.2's defense of drop-and-continue is: "a depletion that arrives before its replenishing receipt fails… under Path A the equivalent caller would have RAISED at submission time." That justification does not survive §15's cross-chunk concession. With chunking, a depletion enqueued *after* its receipt can execute *before* it (chunk assignment preserves enqueue order; claim/execution order doesn't, and pool_lock serializes without ordering). So the invariant "fails only if it genuinely raced its receipt" is false — a caller who did everything in the right order gets a spurious InsufficientInventory. And because routed caller observability is out of scope, the failure mode is: **no trx row ever appears, no error is ever raised**. Silent loss of a well-formed business event, detectable only by polling absence and resubmitting — and the resubmission is subject to the same race.

This bites hardest exactly where routed flavor is supposed to win: hot pool → large connected component → guaranteed chunking at `batch_size_max=200`.

Cheap mitigation for the dominant case: for chunks split from one component, the chunks of a *single hot pool* are already fully serialized by pool_lock — adding a predecessor-wait (chunk N+1 claimable only after chunk N commits) costs zero parallelism there; it only constrains claim order. The general multi-pool-component case is harder (presumably why predecessor-wait was dropped), but a per-pool chain covers S5/S7/S8-shaped workloads. At minimum, §14.2's "preserves strict-time-order semantics" paragraph should be rewritten — it's contradicted two sections later.

### 3. Enqueue is non-transactional w.r.t. the caller's tx in ways the xid triage doesn't cover

The shmem push survives things the caller's tx doesn't:

- **Savepoint rollback.** Caller enqueues inside a subtransaction, rolls back to the savepoint, top-level tx commits. The descriptor is already in shmem and can't be retracted. Whether the ledger records a phantom trx now depends on *which* xid was captured: top-level xid → phantom trx recorded; subtransaction xid → `pg_xact_status` correctly reports aborted. The spec says only "caller's user_tx_xid." This must be pinned to the subtransaction's xid (i.e., `GetCurrentTransactionId`, not top), and stated.
- **`synchronous_commit=off`.** clog says committed before WAL flush; committer records the trx durably; crash loses the caller's business commit → ledger trx for a vanished business event. One sentence noting the triage assumes durable-on-commit is worth it.
- **xid forcing.** Capturing an xid forces assignment on an otherwise read-only tx. For the standalone-enqueue pattern (which is exactly the harness's 1000-caller shape and the likely production shape for a pure event feed), every submission burns a permanent xid with zero WAL written — at the throughputs routed targets, that's cluster-wide anti-wraparound vacuum pressure that "no DB write at submission" (§6.1) elides. `pg_current_xact_id_if_assigned()` semantics fix it: no xid ⇒ no writes ⇒ abort is inconsequential ⇒ triage as always-keep.

---

## Significant

### 4. The headline "1000 → 1" claim is stale against the shipped defaults

§6.7, §9.3's second primary measurement, §11.3, the Phase-3 deliverable, and README "What the PoC measures #2" all say 1000 concurrent submissions to one hot pool become **one** commit_group / one pool_lock acquisition / one aggregate UPDATE. With the acct-p1al default `batch_size_max=200`, they become **five** groups, five acquisitions, five fsyncs, serialized on pool_lock with three committers idle. Still a 200:1 win, but the flagship number repeated in five places contradicts §6.3/§15. Either the tests override the GUC (state it) or the claims need updating. Same for §6.6's "1000:1 reduction."

### 5. Backpressure ↔ eject interlock is a congestion-collapse mode, and it's unsized

Ejected entries return to `pending` — they don't free slots. A caller mid-tx blocked in `ledger_enqueue_trx_c` waiting for ring space is, by definition, in-progress, so *its own earlier entries can never drain*. If the ring fills with entries whose owning txs are all blocked enqueueing their next submission, nothing progresses until `queue_full_timeout_ms` mass-aborts callers, whose entries then triage as aborted and free. Self-clearing, but via a 5-second abort storm. The implied sizing rule — staging capacity must exceed (max concurrent open caller txs × submissions per tx) — appears nowhere, and no scenario in S1–S21 drives queue-full with multi-enqueue-per-tx callers. Also note the backpressure block happens while the caller's tx holds its business-side row locks: ledger overload amplifies into application-wide lock-hold.

### 6. Routed durability asymmetry deserves a blunter statement

The contract is: trx ⟹ caller committed (modulo #3), but caller committed ⇏ trx (postmaster crash, poison, spurious drop per #2). Recovery is "caller observes absence and resubmits" — which means a production caller **must maintain a durable record of pending submissions and a reconciliation poller**, i.e., a transactional outbox. Once the caller has an outbox, the shmem queue is a latency/batching optimization over "poll the outbox," not a reliability primitive. That's a fine position, but §6.1's framing ("caller can resubmit") makes it sound lighter than it is. It also affects the crossover analysis (§11.4): direct-batched has none of this caller-side machinery cost, which is a real weight on routed's side of the scale that throughput numbers won't show.

### 7. pool_lock row `FOR UPDATE` vs advisory locks — never discussed

Every acquisition writes the pool_lock tuple's xmax (XLOG_HEAP_LOCK WAL record, page dirty). On a hot pool that's per-tx WAL and checkpoint churn on one page — nonzero noise in a PoC whose headline metric is lock-hold time. `pg_advisory_xact_lock(pool_id)` gives identical blocking/release-at-commit semantics with zero WAL, no lazy-create dance (§5.1 step 2 disappears entirely), no table. The costs (keyspace collision with other advisory users, less introspectable) are real but minor. The doc should at least record why the row-lock design won; right now the alternative is unexamined.

### 8. qty's fixed-point scale is unspecified, and the unit_cost derivation depends on it

§3.0 pins monetary values at 1e-6 but only says quantities are "BIGINT with implicit fixed-point precision." `unit_cost = banker_div(value_sum, qty)` yields cost per **qty-LSB**. If qty is also 1e-6 fixed-point, a $1.00/each item has a true per-qty-LSB cost of 1 BIGINT-unit and a $0.50/each item rounds to 0 or 1 — the running average collapses to garbage at the rounding cliff for ordinary prices. Everything works only if qty is integral whole units (which the code presumably assumes). Say so explicitly, or the §3.0 precision analysis is incomplete. Relatedly: the i128→i64 downcast is specified for `banker_div` but not for **storing** `new_value_sum` — the accumulation path needs the same try_from/error posture, unstated.

### 9. Standard revision needs a companion revaluation posting; the spec is silent

§3.3/§2.2 describe in-place standard_cost updates and aggregate mirroring "at the next receipt or depletion." Nothing generates a revaluation posting for on-hand qty at revision time, so subledger value (qty × new std) diverges from the GL inventory balance (postings at old std) until someone manually posts a `revaluation_run` trx. The enum and posting_account_map columns exist, but the operational coupling — "revising standard_cost without a revaluation trx breaks GL⇄subledger reconciliation" — is never stated. STD's value_sum maintenance is also unspecified (§3.3 says the aggregate is "qty + unit_cost" but the column is NOT NULL and CHECKed — see finding 1's second scenario).

### 10. Idempotency key conflates document identity with event identity

UNIQUE (trx_type, source_id) means one ledger event per business document per type, ever. A corrected/reopened PO receipt, a second adjustment against the same document, or two upstream systems sharing an id space all collide. Real systems key idempotency on a submission/event id, not the doc id. Also: dedup is first-write-wins with no payload comparison — a caller that retries with *mutated* lines (retry bug, partial fix-up) gets a silent skip, not a conflict error. A payload hash in the dedup would catch it cheaply.

---

## Measurement validity

### 11. The direct-flavor headline result is true by construction

§11.2's constant-lock-hold-vs-depth property is near-tautological: the hot path never reads layer rows, so seeded depth can only affect it through second-order effects (btree depth on pool_state's PK at 10M rows — marginal). The doc admits this ("self-referential"), but the consequence should be stated plainly: v3.1 de-risks the cheap half of Path C and defers the entire load-bearing risk (recalc/close feasibility, §14.6 watermark correctness, layer materialization cost, variance magnitude) to a future phase. The PoC's real evidentiary value is the routed machinery's correctness under load — which is substantial — not the Path C premise.

One cheap, high-value addition: variance magnitude doesn't need production fifo.rs. A throwaway offline replay (even a Python script over a trx_line dump) against the harness's own runs would give provisional-vs-true-FIFO variance distributions per basis and volatility profile. That's the number the *business* case for Path C hangs on, and it's currently completely unmeasured for ~an afternoon of effort.

### 12. pgbouncer transaction pooling confounds the 1000-caller scenarios

S5/S7/S8 run through a transaction pooler because the container can't hold 1000 backends. Then direct flavor's "1000 concurrent callers" is actually pool-size concurrent transactions — the contention the crossover analysis measures is shaped by an unstated pgbouncer pool size, not by 1000-way lock queuing. Routed enqueue is barely affected (fast calls), so the confound is asymmetric and flatters routed. The pool size needs to appear in the report next to every S5/S7/S8 number, and ideally a sensitivity sweep over it.

### 13. Routed latency floor is self-imposed

50ms latch timeout + 500μs dwell means routed ack latency has a ~25ms mean floor from tick alignment alone, against direct's microsecond critical section. Nothing prevents the enqueue path from `SetLatch`-ing the router for immediate wake (dwell gate still applies). If crossover conclusions include latency, they're currently measuring the tick constant, not the architecture.

---

## Moderate / minor

**14.** Recovery mechanism described inconsistently: README P3.4 says the router boot sweep flips CQ `in_flight→ready` for dead owners; §6.5 says a live committer CAS-swaps `(slot_idx, generation)` leaving state `in_flight`. If both paths exist (boot vs steady-state), say so; as written they read as contradictory.

**15.** Liveness hole in dead-committer detection: committer dies, its slot's generation never advances (nobody reclaims), OS recycles the pid to an unrelated process → `kill(pid,0)` succeeds, generation matches → the in_flight group is unreclaimable until postmaster restart. Fine if committers are registered with BGWorker auto-restart (the restart bumps the generation) — but that assumption is load-bearing and unstated. There's also no age-based timeout on `in_flight` as a backstop.

**16.** posting_line's paired debit/credit-per-row model: workable (the §3.7 direction rule and the two-row STD receipt compose correctly — I checked the AP amounts balance), but posting_line_dimension attaches to the *pair*, so you can't dimension the debit leg differently from the credit leg (cost_center on the expense side only). Real GL limitation; one-row-per-leg with signed amount is the conventional fix. Also trial-balance queries need a UNION over both account columns.

**17.** No (pool_id, id) composite index on trx_line. §14.1 tells queries to "replay trx_line directly" and recalc/close will walk per-pool streams in id order; `trx_line_pool(pool_id)` alone forces a sort. Trivial to add now, painful to add against a large table later.

**18.** Poisoned commit_groups live only in shmem — postmaster restart erases not just the submissions but the *evidence* they were poisoned. The PoC metric is fine; note that production dead-lettering must be a table, or the "operator review" posture in §6.8 is vapor.

**19.** Input constraints under-specified at the schema level: no CHECK against qty=0 trx_lines (which make the §3.7 sign-based direction rule undefined), nothing about negative unit_cost receipts. ledger-core presumably guards; the spec should say where the authority lives.

**20.** README nits: `committer_count` labeled Sighup with a footnote that reload does nothing — that's Postmaster scope in practice, label it so. Staging state 4 = `abandoned` and CQ `valid==4` = poisoned reuse the same code in two enums; cosmetic but a grep hazard. The sizing-GUC trap is well-documented and test-pinned, but GUCs that accept values and ignore them remain a footgun — consider making mismatch a startup FATAL instead of NOTICE.

---

## What holds up well

The value_sum-exact/derived-average model (§3.0) is the right call and the receipts-only order-independence proof is correct. §14.6's identity-allocation-vs-commit-order warning is exactly the trap a future recalc/close author would fall into. The pre-flight-dedup + 23505-re-drive layering (§6.4/§6.8) is sound and the cross-commit-group race analysis in §6.4 step 5 is accurate. The pgbouncer/max_worker_processes/arena-leak disclosures show the audit culture is real. My pushback is concentrated on: finding 1 (which I'd fix before trusting any standard-basis or STD numbers), the stale 1000→1 claims, and the framing gap between what the PoC proves and what Path C needs proven.
