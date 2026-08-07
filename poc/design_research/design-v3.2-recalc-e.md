# design-v3.2 recalc engine (e): close-time semantics — finalize provisional to authoritative valuation

> **Status: DESIGN (acct-q1oj.5, 2026-07-10).** The finalize child of the v3.2 recalc/close workstream
> (acct-q1oj / `design-v3.2.md` §5) — the last of the five. Turns mid-period provisional / standard-valued
> reporting into a period-end authoritative valuation, and gates the accounting-period transition on recalc
> having caught up. Consumes (c)'s backlog gauges (`design-v3.2-recalc-c.md`), (d)'s
> `cost_settlement`/`pool_settlement` (`design-v3.2-recalc-d.md`), and (b)'s materialized layer state. Owns
> the accounting-period close hook and the variance-into-empty-pool sweep (d §6 handed it here). Design-only.

## 1. What "close" is under alt C — and what it is not

Under §16 the mid-period GL is qty-only or standard-valued (the SAP material-ledger shape); recalc's
authoritative valuation lands at close. Two framings must be kept straight (§19):

- **It is NOT an adjustment storm.** There is one costing plane (§16) — the hot path posts no cost leg, so
  there is nothing to *reverse* at close. The valuation move is the **first and only** authoritative cost for
  the period's depletions, sized by the acct-0at4.7 drift (§2b: biased under a trend, worst for standard basis
  at rel 61.72 %), surfaced as (c)'s G2b.
- **It is a consistency gate + a finalize stamp.** "Close" = ensure every pool with in-period activity is
  authoritatively costed through the period end, sweep any residue, then stamp the period's valuation
  authoritative and **immutable**.

Because recalc runs continuously (c §3), by close time most pools are already settled; close is the final
check + waiting on stragglers (the Pareto-hot pools, c §4), not a from-scratch batch.

## 2. The accounting-period state machine + the close hook

`accounting_period.state ∈ {open, closing, closed}` already exists (§2.4); the close *hook* does not (§7/§13:
the table exists, hooks are unimplemented in the PoC). This child adds it, mirroring the parent `acct` repo's
orchestrated-close posture (a `close_period(...)` function that runs gates then stamps — not a manual
`UPDATE`):

```
close_period(p_period_id BIGINT, p_actor TEXT, p_force BOOLEAN) RETURNS <close report>
```

1. `open → closing`: mark the period `closing` under `FOR UPDATE` on the period row (serializes concurrent
   close callers, as the parent's `close_period` does). Appends continue — `closing` means "draining recalc
   for finalize," not "frozen."
2. **Recalc-drain gate** (§3). If it passes, or `p_force` forces a synchronous drain, proceed; else block.
3. **Variance-into-empty-pool sweep** (§5).
4. **Finalize + immutability** (§4): stamp `closed_at`/`closed_by`, `state → closed`, freeze the period's
   authoritative valuation.

## 3. The recalc-drain gate (on (c)'s G2a)

The period `P = [start_date, end_date]` is drain-complete iff **every pool with an event `posted_at ≤
end_date` is authoritatively costed through `end_date`**. Concretely, over pools with in-period activity:

- no pool has a `recost_floor` at `posted_at ≤ end_date` (no pending backdated re-cost in the period), **and**
- every such pool's `pool_settlement.settled_through_posted_at ≥ end_date` (equivalently, its in-period tail is
  settled).

This is (c)'s **G2a** (per-pool `settled_through` lag) scoped to the period. Events `posted_at > end_date`
belong to the next period and may stay un-costed — the gate is period-scoped via the R-1 chronological key
(child (a): order by `posted_at`), so "settled through `end_date`" is well-defined.

- **Gate passes** → finalize (§4/§5).
- **Gate fails** (stragglers draining) → the close **blocks** (waits for continuous recalc to catch up) unless
  the caller forces (§6).

## 4. Finalize = authoritative + immutable

On a passing gate (or a completed forced drain):

- The period's `cost_settlement` rows (d) for depletions `posted_at ≤ end_date` become the **finalized
  authoritative valuation** — the max-generation row per depletion, frozen. Mark them finalized (a period
  stamp or a `finalized` flag on the settlement row).
- Stamp `accounting_period.closed_at` / `closed_by`; `state → closed`.

**Immutability is a schema invariant, not API discipline** (adopting the parent repo's load-bearing posture).
After a period closes, no new `recalc_generation` may re-cost an in-period depletion — that would be a silent
reopen. Enforce with a gate/trigger that **rejects** a new `cost_settlement` generation (or a backdated
`trx_line`) whose `posted_at ≤` a closed period's `end_date`:

> **Backdated-into-closed-period is rejected** (a `PeriodClosed` error, analogous to the parent repo's P0021).
> Re-costing a closed period requires a **period-reopen workflow, which is out of scope (§13)** — the same
> boundary that let (c) resolve D11 (no historical checkpoints). Consistent across the two children: no
> reopen ⇒ no backdate-into-closed, ⇒ no historical checkpoint need.

## 5. Variance-into-empty-pool sweep (from d §6)

(d §6) provided the hook and handed the *timing* here. The residue is (i) banker-rounding residue across a
pool's depletions, or (ii) authoritative cost with no surviving layer to absorb it (the pool emptied
in-period). Design:

> **Sweep per in-period pool at finalize (not per pass).** For each pool with in-period activity, compute the
> residue = `Σ authoritative depletion value − Σ consumed receipt-layer value − remaining open-layer value`
> and post it as a single `cost_adjustment_line` against the pool's variance account (`posting_account_map.
> variance_acct`, §3.7), with `pool_state.value_sum` absorbing it as the aggregate figure (allowed negative,
> 0009). Doing it once at close (not on every mid-period pass) keeps forward passes clean and makes `value_sum`
> **exact at period end** — the reconciliation point conservation (acct-0at4.5) checks.

If `variance_acct` is NULL for a pool that needs a residue posting, fail loud (`MissingVarianceAccount`, §3.7)
— same posture as the hot path.

## 6. Forced close — drain synchronously, do NOT skip

If close must proceed before continuous recalc drains (a statutory deadline), `p_force = TRUE`. Crucially,
under alt C **force means drain-synchronously, not bypass-the-gate** — a departure from the parent repo's
`p_force_provisional`, which *skips* a gate and leaves provisional rows:

- There is **no provisional cost leg** to fall back on (§16/§19: "ahead of recalc the value plane simply does
  not exist yet"). A period closed with un-costed in-period depletions would have **no valuation** for them —
  incoherent. So force cannot mean "close anyway, leave them un-costed."
- Force therefore **drains the period's lagging pools synchronously** as part of the close (all recalc workers
  + the close caller burst through the remaining dirty in-period pools), then finalizes. This *is* the SAP
  CKMLCP forced-close precedent (§19): forcing pays the whole accumulated fold **now**, at close latency —
  potentially long for a deep Pareto-hot pool (b §2, the 25-events/s floor), which is the honest cost, not a
  design defect.

**Operator signal — no silent forced close.** Before forcing, surface (c)'s **G2b** (the sized pending move —
`un-costed-tail value × oracle rel%`, directional under a trend) and **G2a** (which pools lag and by how much),
so the operator sees the magnitude and the wait they are committing to. The close report returns them.

## 7. The three §7 worker-model shapes are points on one design, not alternatives

§7 named Oracle continuous / SAP on-demand / Dynamics periodic as competing implementation options. Under the
surviving architecture they **coexist** as facets of (c)'s cadence + (e)'s close surface:

| §7 shape | maps to |
|----------|---------|
| Oracle continuous worker | (c)'s default continuous drain — recalc always running |
| Dynamics periodic Inventory-Close gating the period transition | **(e)'s `close_period` gate** (§3) + scheduled boundary sweep |
| SAP on-demand `ledger_settle_pool()` before a statutory query | an SPI **(e)** adds: `settle_pool(p_pool_id)` force-drains one pool to its stream head synchronously |

So (e) exposes `close_period(period, actor, force)` [periodic] and `settle_pool(pool_id)` [on-demand], both
riding (c)'s continuous drain [continuous]. A deployment picks its posture by *how it calls these*, not by a
different engine.

## 8. Function / SPI surface (sketch)

- `close_period(p_period_id, p_actor, p_force)` — `FOR UPDATE` period row; gate on G2a (§3); on pass/forced
  drain, sweep residue (§5), finalize + stamp (§4). Idempotent: re-running on a `closed` period is a no-op /
  `AlreadyClosed`. Concurrent callers serialize on the period row.
- `settle_pool(p_pool_id)` — force-drain one pool to head synchronously (SAP on-demand, §7).
- Immutability enforcement (§4) — a trigger/gate rejecting new settlement generations or backdated `trx_line`s
  into a closed period (`PeriodClosed`).

## 9. Correctness / validation

- **Post-close completeness:** every in-period depletion has a finalized authoritative `cost_settlement`; the
  period's Σ reconciles (conservation, acct-0at4.5); `value_sum` is exact per pool after the §5 sweep.
- **Gate correctness:** `close_period` blocks while any in-period pool lags (`settled_through < end_date` or a
  `recost_floor ≤ end_date`), and passes exactly when all are settled.
- **Forced close:** drains the lagging in-period pools synchronously, emits the G2b-sized move, finalizes; the
  close report exposed the magnitude first (no silent force).
- **Immutability:** a backdated event / new generation into a closed period is rejected (`PeriodClosed`,
  reopen out of scope §13).
- **Idempotent close:** re-running `close_period` on a closed period is a no-op.
- **Worker-model coexistence:** `settle_pool` (on-demand) + continuous drain + `close_period` (periodic) over
  the same pools produce identical finalized valuations (all three are the same generation-delta engine, d).

## 10. Interfaces and open items

- **← (a)/(b)/(c)/(d)** — consumes materialized layer state (b), `cost_settlement`/`pool_settlement` (d), and
  the G2a/G2b gauges (c); this child is the terminal consumer that freezes their output.
- **Open — all operational, none change correctness:**
  - **D12** — forced-close granularity: all-workers-synchronous vs bounded burst (operational).
  - **D13** — immutability enforcement mechanism: trigger (schema invariant, **recommended** per the parent
    repo's "period lock is a schema invariant, not API discipline") vs orchestration-function-only.
  - **D14** — `PeriodClosed` reject vs a future period-reopen hook: reject in v3.2 (reopen out of scope §13);
    the reject site is where a reopen workflow would later attach.

With (e) designed, the recalc/close engine decomposition (a)–(e) is complete: (a) the per-pool layer-walk +
R-1/R-2, (b) the cross-pool scheduler + checkpoint/materialization, (c) the cadence/backlog control + gauges,
(d) the state schema + generation-delta idempotency, (e) the close-time finalize. Every optimization across the
five is validated equal to (a)'s full-opening replay, which is the acct-0at4.7 oracle — so the whole engine's
correctness reduces to that one golden reference.
