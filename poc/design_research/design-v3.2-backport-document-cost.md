# Backport design: the document-cost read model under two-phase costing

> **Status: design note (dossier).** Deliverable of `acct-476a.4`. This specifies how a document
> layer reads "the cost of this line" from a v3.2 costing plane, and restates the parent repo's **R7**
> audit-field rule for a plane where cost arrives in two phases. It designs a contract; it implements
> nothing.
>
> With `acct-476a.2` (reservations under alt-C) this is one of the two experiments convergence **Q2**
> designated to decide asset-vs-rebuild for the `acct` document layer. §6 is this dossier's half of
> that verdict.
>
> Baseline: [`design-v3.2.md`](design-v3.2.md) — the costing plane's spec-of-record. **§5a** there is
> this note's sibling: it governs reads of *inventory value* (a pool's worth). This note governs reads
> of *document cost* (a line's cost of goods). They share a mechanism and differ in what they conclude.
>
> **Shared lifecycle vocabulary**, used identically here and in `acct-476a.2`'s note:
> **`provisional` → `settled` (still revisable) → `final`**. `settled` is never a synonym for done.
> The alignment is bilateral: the reservations dossier's **D-R4** adopts *shipped ≠ cost-final*, so
> neither note uses `settled` where it means `final`.
>
> **The five open questions in §5 were decided on 2026-08-07** — see §7 for the decision record. §5
> retains the arguments; §7 states what was chosen.

## 1. The conflict, precisely

### 1.1 What the parent requires

The parent repo persists cost snapshots on document lines and requires them to agree with the ledger.
`so_shipment_lines` (`db/migrations/0018_outflow_documents.up.sql:86–96`) carries

```sql
unit_cost           BIGINT NOT NULL CHECK (unit_cost >= 0),
cost_method_at_ship cost_method NOT NULL DEFAULT 'standard',
```

**R7** (CLAUDE.md class-confusion checklist; **AP9** in `REVIEW.md`) states the invariant: a document
audit field must come from the post-lock dispatcher's output, so that `*_lines.unit_cost` equals
`posting_line.amount / qty` for the same line. Its failure symptom is precise — *the ledger is right
but the document audit field disagrees with it*. Two bugs motivated it (`acct-5prc` on `post_so_ship`,
`acct-quca` on `post_standard_cost_roll`), both closed by
`db/archive_migrations/0091_audit_loop_closure.up.sql`, and it is enforced continuously **on
`wac_perpetual` SKUs** by `tests/property_ap9_audit_ledger_consistency.rs:168–184`, which asserts
`posting_line.amount == qty_shipped × audit_unit_cost` across random receive/ship sequences. That
scope matters: **no property test currently pins R7 on the parent's `fifo` or `lot` paths**, which are
the paths this note is about.

Three premises hold the invariant up:

1. **The cost is knowable at document time.** Even for FIFO the parent walks layers inline:
   `post_so_ship` takes `FOR UPDATE` on the value account, calls `_fifo_walk_layers`, and sets
   `v_unit_cost := v_walk_total / v_qty_shipped` (`db/migrations/0039_fifo_post_so_ship.up.sql:196–204`).
2. **The cost is final once written** — *for `standard` and `wac_perpetual` only*. See §1.2.
3. **A COGS journal leg exists at document time** to compare the snapshot against.

### 1.2 The parent already runs two-phase costing

Premise 2 is not a universal property of the parent, and this is the single most important fact in
this dossier — it converts alt-C from an alien posture into an extension of something the parent
already does.

For `wac_periodic` and `wac_retroactive` SKUs the parent flags depletions as provisional at write time
and trues them up at close. `_post_posting_lines_apply_event`
(`db/migrations/0014_post_posting_lines.up.sql:279–293`) tests the reason against
`('op_move','scrap','wo_complete','so_ship','op_move_v','scrap_v','wo_complete_v','rm_issue_to_wo')`
— **`so_ship` is explicitly in that list** — resolves the cost method credit-first (R2), and for those
two methods writes a row into `posting_lines_provisional`. That table
(`db/migrations/0012_period_close.up.sql:23–45`) carries `finalized_at`, `variance_amount` and
`variance_posting_line_id`: a provisional-until-close lifecycle, finalized by
`wac_periodic_close_hook` / `wac_retroactive_close_hook`, which post variance for the difference
between the provisional cost and the recomputed one.

So on a `wac_periodic` SKU a shipped line's *cost* is already revised after the document is written.
R7's literal identity survives — the snapshot still equals the original posting line's amount over
qty, because the correction arrives as a *separate variance posting* rather than as an edit — but
"nothing later revises the cost" is false, and the parent already ships the provisional/settled/final
shape §2 proposes.

Two consequences run through the rest of this note. The conflict with alt-C is **narrower** than a
first reading suggests, and the parent's own convention supplies the precedent for how to resolve it:
correct by posting variance, never by rewriting the document's snapshot.

### 1.3 What alt-C provides

Under alt-C (Q3a), for **FIFO/LIFO pools**:

1. The hot path records the physical event and an *observed provisional* cost —
   `trx_line.unit_cost`, the running average or standard basis (design-v3.2 §3).
2. The authoritative cost is the **max-generation `cost_settlement` row**
   (`poc/ledger-v3.2/db/migrations/0009_recalc_tables.up.sql:57–66`), and it keeps moving: a
   backdated event lowers the pool's recost floor and R-2 forces a fresh generation for every
   affected depletion behind it.
3. **There is no hot-path COGS leg at all** — see §1.5. This is where alt-C goes further than the
   parent's provisional lifecycle, which does post a full cost leg at write time.

How far apart the two numbers sit is measured, not assumed. Under ±40% receipt-cost volatility with
5% backdating the provisional plane is **~22–30% wrong per depletion** on average until recalc trues
it up (`poc/ledger-v3.2/bench/soak-results.md` §3). The error is *directional*, so it accumulates
rather than cancelling: measuring **Δ = authoritative − provisional**, FIFO's Δ is negative (the
provisional plane **overstates**), while under a monotone rising-cost profile LIFO's Δ flips positive
(the provisional plane **understates**). That trend profile is a different workload shape — 0.5×→1.5×
rising cost at 10% backdating — and reaches 36.0% on FIFO.

### 1.4 Which methods are actually in conflict

R7 is written as a universal rule, which hides how narrow the conflict is.

In v3.2, **WAC, STD and specific-id stay final on the hot path** (Q3a). WAC is final under PG's own
tuple lock on the aggregate `pool_state` row inside the single-statement CTE
(`poc/ledger-v3.2/ledger-direct/src/cte.rs:1–7`); STD and specific-id are final under `ledger-core`'s
pool-row locks via `plan_apply` (design-v3.2 §3). Different mechanisms, same outcome: cost and cost leg
land together, and R7 holds verbatim. **Two of v3.2's five pool methods are in conflict.**

That count does not transfer to the parent, whose `cost_method` enum has seven values
(`db/migrations/0002_types_and_enums.up.sql:184–191`, plus `lot_fifo` from `0045`). Mapped over:

| Parent method | Status |
|---|---|
| `standard`, `wac_perpetual` | Keep R7's simultaneity form verbatim |
| `wac_periodic`, `wac_retroactive` | **Already two-phase close-settled** (§1.2) — precedent, not conflict |
| `fifo`, `lot`, `lot_fifo` | In conflict: layer-tracked, synchronous today |

So the parity question is not "can a document layer live on v3.2" but the much narrower **"will the
document layer give up synchronous cost at write time for its three layer-tracked methods, having
already given up final-cost-at-write-time for two others?"**

### 1.5 The journal is not a complete COGS record for FIFO/LIFO

The structural fact that makes a read model necessary rather than merely convenient.

For a FIFO/LIFO depletion the v3.2 hot path posts **no `posting_line`**
(`poc/ledger-v3.2/ledger-direct/src/cte.rs:20,102,130`; design-v3.2 §3's per-method table). The only
journal rows such a depletion ever acquires come from the recalc engine, and their amount is the
*inter-generation delta*: at generation 1 the engine takes `prior = observed_unit_cost` and posts
`(authoritative − observed) × qty` (`poc/ledger-v3.2/ledger-direct/src/recalc.rs:188–196`).

This is deliberate, not a gap: the bench verifier's **V5** asserts it as a structural invariant —
*"FL physical lines post no hot-path GL; wac physical lines post exactly one"*
(`poc/ledger-v3.2/ledger-bench/src/verify.rs:14–17`) — and **V2**'s value identity (`:9–13`) is written
in terms of telescoped settlement deltas, not journal totals.

Note what this implies about the `prior = observed` choice: it is exactly right *when the observed
cost was journalled*, which is true for WAC and for the parent's provisional lifecycle, and leaves the
journal incomplete when it was not. The parent's `wac_periodic` path posts the full provisional cost
and then a variance; v3.2's FIFO/LIFO path posts only the variance. §5.4 is that gap.

The consequence for R7 is categorical. For a FIFO/LIFO line, `posting_line.amount / qty` is not a
wrong cost — **it is not a cost at all**. R7's right-hand side does not exist, so R7 cannot be repaired
by locking harder or reading later. It has to be restated against a different authority (§4).

### 1.6 What close changes

Closed-period cost is **final by construction**. Migration `0017`'s `BEFORE INSERT` guards freeze the
period's replay input, and the argument is recorded in its header
(`poc/ledger-v3.2/db/migrations/0017_period_close_guards.up.sql:1–20`): the strict fold is
*prefix-stable* — a depletion's authoritative cost depends only on events at or before its R-1
position — so an unchanged physical prefix means the engine can never recompute a different
authoritative cost for a closed period's depletions. A companion guard on `cost_settlement` rejects
any attempt as a fail-loud backstop.

Finality is therefore a real, checkable state — stronger than the parent's, where nothing structurally
prevents a later write.

## 2. The three states

The issue asks for "a settled-vs-provisional status flag". Two states would build in exactly the wrong
assumption — that *settled* means *done*.

| Status | Condition | What it means | Revisable? |
|---|---|---|---|
| `provisional` | No `cost_settlement` row for this depletion | Recalc has not costed it. The only number available is the observed hot-path cost | Yes — no authoritative value exists yet |
| `settled` | A max-generation `cost_settlement` row exists | The engine's current authoritative answer | **Yes** — a backdated event behind it forces a new generation (R-2) |
| `final` | The depletion's `posted_at` falls in a **closed** period | Frozen by schema invariant (§1.6) | No |

The trap is the middle row. `settled` is the engine's best answer *given the events it has seen*; only
`final` is a promise. A consumer that treats `settled` as final publishes numbers that later change —
the failure R7 exists to prevent, reintroduced one level up. The parent's vocabulary maps cleanly:
`posting_lines_provisional` with `finalized_at IS NULL` is `provisional`/`settled`; `finalized_at`
stamped at close is `final`.

One qualifier rides alongside: a pool may carry a **recost floor** at or below a depletion's position,
meaning a pass is already scheduled to revise it. That is not a fourth status — the row is still
`settled`, just known-stale — so it is exposed as a separate boolean. Cost reporting can treat
`settled AND NOT recost_pending` as "stable enough to show".

Note the level shift: `settled_through` is a **pool-level** frontier used by the G2 gauges, whereas the
existence of a settlement row is the **line-level** fact. The status reads the line-level fact, which
is the one that cannot be stale for the wrong reason.

## 3. The read model

### 3.1 The view

The authoritative read is a max-generation lookup the engine already performs internally —
`load_priors` (`poc/ledger-v3.2/ledger-direct/src/recalc.rs:498–528`) runs
`SELECT DISTINCT ON (depletion_trx_line_id) … ORDER BY depletion_trx_line_id, recalc_generation DESC`.
The view generalizes that query into a public interface. No new state, no denormalization, no second
home for any fact.

```sql
CREATE VIEW document_cost AS
SELECT tl.id                      AS depletion_trx_line_id,
       tx.trx_type,
       tx.source_id,                                    -- the document layer's key
       tl.pool_id,
       p.method,
       tl.posted_at,
       -tl.qty                    AS qty_depleted,      -- depletions carry qty < 0
       tl.unit_cost               AS observed_unit_cost,
       CASE WHEN p.method IN ('fifo','lifo') THEN cs.authoritative_unit_cost
            ELSE tl.unit_cost                           -- final on the hot path
       END                        AS authoritative_unit_cost,
       cs.recalc_generation,
       COALESCE(cs.authoritative_unit_cost, tl.unit_cost) AS effective_unit_cost,
       CASE
           WHEN p.method NOT IN ('fifo','lifo') THEN
                CASE WHEN ap.state = 'closed' THEN 'final' ELSE 'settled' END
           WHEN cs.recalc_generation IS NULL THEN 'provisional'
           WHEN ap.state = 'closed'          THEN 'final'
           ELSE                                   'settled'
       END                        AS cost_status,
       (ps.recost_floor_posted_at IS NOT NULL
        AND (ps.recost_floor_posted_at, ps.recost_floor_id)
            <= (tl.posted_at, tl.id))          AS recost_pending
FROM trx_line tl
JOIN trx  tx ON tx.id = tl.trx_id
JOIN pool p  ON p.id  = tl.pool_id
LEFT JOIN pool_settlement ps ON ps.pool_id = tl.pool_id
LEFT JOIN LATERAL (
    SELECT c.authoritative_unit_cost, c.recalc_generation
    FROM cost_settlement c
    WHERE c.depletion_trx_line_id = tl.id
    ORDER BY c.recalc_generation DESC
    LIMIT 1
) cs ON true
LEFT JOIN accounting_period ap
       ON tl.posted_at >= (ap.start_date::timestamp     AT TIME ZONE 'UTC')
      AND tl.posted_at <  ((ap.end_date + 1)::timestamp AT TIME ZONE 'UTC')
WHERE tl.qty < 0
  AND tl.line_type <> 'cost_adjustment_line';
```

Five details are load-bearing:

- **The status is method-aware, because settlement state exists only for FIFO/LIFO.** The engine skips
  every other method (`recalc.rs:136` — `if claim.method != "fifo" && claim.method != "lifo"` →
  `"skipped": "non_strict_method"`), `gate_pools` scopes to `p.method IN ('fifo','lifo')`, and V7
  asserts *"wac pools have no settlement state"* (`verify.rs:21`). A method-blind view would report
  every WAC line as `provisional` forever, and — worse — as `final` with a NULL authoritative cost
  once its period closed. For non-layer-tracked methods the observed cost **is** the authoritative
  journalled cost, which is what the two `CASE` arms encode. Filtering the view to FIFO/LIFO instead
  is defensible, but it would force every consumer to branch on method before choosing a query; making
  the view total is the better interface.
- **A missing settlement row is never reported as `final`.** The FIFO/LIFO arm tests
  `cs.recalc_generation IS NULL` *before* it tests the period state. A closed period should contain no
  unsettled FIFO/LIFO depletion — the close gate drains, and force drains synchronously — so this
  ordering makes an anomaly visible as `provisional`-in-a-closed-period rather than laundering it into
  a `final` NULL.
- **The period join carries its own timezone pin.** This is a **new** DATE→timestamptz boundary cast
  site, outside the seven that `acct-1vur.5` enumerated, so it must be pinned explicitly — it inherits
  nothing. The sketch above uses `(date_col::timestamp AT TIME ZONE 'UTC')`, matching the form that
  migration `0024_timezone_pinned_boundaries` landed in the guard bodies. As a
  public read interface evaluated in *arbitrary consumer sessions*, it is the most timezone-exposed
  cast site in the plane, not the least: a client that sends its own `TimeZone` (JDBC does by default)
  overrides even a cluster-level `timezone='UTC'` pin. The bracket convention itself —
  `[start_date, end_date + 1)` — matches `0017`'s guards and the close gate. (It does *not* match "the
  sweep", which has no date predicate at all; `sweep_pool` inherits its pool list from `gate_pools`.)
- **`qty < 0` selects depletions**, per the strict fold's treatment of `qty > 0` as a layer-forming
  receipt (`poc/ledger-v3.2/ledger-core/src/strict.rs:92`). Only depletions have a cost to read.
- **`cost_adjustment_line` rows are excluded** — engine output, stamped `posted_at = now()`, never
  replay input; the same exclusion migration `0015` applies to the G2 gauges.

`effective_unit_cost` always returns a number, so a consumer that must render something cannot
accidentally render nothing; `cost_status` is what tells it whether the number is safe.

### 3.2 The GL tie-out

Because the base COGS is unjournalled for FIFO/LIFO (§1.5), the journal-side identity is a
*telescoping* one rather than an equality:

```
Σ (adjustment posting amounts across generations)  =  (authoritative_maxgen − observed) × qty
```

Intermediate generations cancel, which is the point of the generation-delta model (design-v3.2 §5.2).
Two mechanics matter for anyone building the reconciliation report:

- It joins **`cost_settlement` directly, across all generations**, on `adjustment_posting_line_id` —
  not the `document_cost` view, which exposes only the max-generation row and so cannot sum the series.
- It must **exclude zero-qty sweep-residue rows**: migration `0016` permits `qty = 0`
  `cost_adjustment_line` rows for the close sweep's per-pool residue GL, which belongs to no
  depletion's series.

It must not check `posting_line.amount / qty == unit_cost` — that is the parent's identity and is false
here by construction.

### 3.3 The consumer contract

| Consumer | Reads | At which status |
|---|---|---|
| Financial reporting / period COGS | `authoritative_unit_cost` | `final` only |
| **Invoice COGS** (the cost side of an invoice) | `authoritative_unit_cost` if pinning, `effective_unit_cost` if floating | **governed by §5.1 — open** |
| Invoice pricing (the price side) | unaffected — sales price is asserted, not pool-derived | any |
| Management COGS, margin dashboards | `effective_unit_cost` + `cost_status` | any, must display the status |
| **Return credits** (`post_customer_return`) | must **re-resolve through the view**, not copy the ship snapshot | see below |
| Inventory valuation | not this view — design-v3.2 §5a's layer read | — |
| Audit trail ("what did we believe then") | `observed_unit_cost` | any |

The rule that falls out: **anything that must tie to the general ledger reads `final`.** Everything
else reads `effective_unit_cost` and is obliged to carry the status with it. This mirrors §5a's
conclusion for the valuation plane — one posture, applied at two levels.

**The return path is a live second-order hazard.** `post_customer_return` copies the shipment's cost
snapshot straight into the credit: `INSERT INTO customer_return_lines … VALUES (…, v_sl.unit_cost, …)`
(`db/migrations/0018_outflow_documents.up.sql:1476–1480`). Under a port that renames the ship column to
`observed_unit_cost`, an unchanged return path would credit a provisional cost that is ~22–30% off.
This consumer must re-resolve through the view; it is not a rename site.

**The flush-wipe divergence does not reach this plane.** §5a's `v2_skipped_flush_wiped` concession
(8 of 300 pools in the headline soak) is a property of the *aggregate* `value_sum`: the hot path's
exact-empty flush discards engine corrections folded into it (`soak-results.md` §6). `cost_settlement`
rows are untouched. A document-cost reader is therefore immune, and this note inherits no caveat.
Recorded because the inverse would be a serious gap.

## 4. R7, restated

### 4.1 The rule

> **R7 (two-phase form).** A document-level cost field must resolve through the settlement view, never
> snapshot the provisional observed cost as if it were authoritative. Where a document persists a cost
> at write time, that column is **the observed cost, named as such**, and carries no claim of agreeing
> with the ledger. The document's *authoritative* cost is a read, not a column.

The original rule's substance survives — a document must never publish a cost that disagrees with the
ledger — but its mechanism inverts. R7 as written is a **simultaneity** rule: compute under the lock so
snapshot and ledger agree *at the instant of writing*. Under two-phase costing there is no such
instant. It becomes a **convergence** rule: the document's authoritative read tracks the settlement
view, which converges and then freezes at close.

The parent has already accepted a limited form of this. Its `posting_lines_provisional` lifecycle
(§1.2) keeps the literal identity intact by correcting through variance postings rather than editing
the snapshot — the same discipline this restatement generalizes. For `standard` and `wac_perpetual` the
original form applies unchanged.

### 4.2 What happens to the parent's snapshot columns

The pool-derived cost snapshots are the migration surface. Enumerating them is what bounds the work —
and the enumeration must span **line and header tables both**, because the natural grep shape
(`INSERT INTO *_lines (…, unit_cost, …)` across a post-lock barrier, the pattern `REVIEW.md` records)
structurally cannot reach a header-level document table.

| Column | Shape | Under a port |
|---|---|---|
| `so_shipment_lines.unit_cost` (`NOT NULL`) | pool-derived — the canonical R7 case | Rename to `observed_unit_cost`; authoritative cost via the view |
| `so_shipment_lines.cost_method_at_ship` (`NOT NULL`) | method snapshot | Keep — the method is genuinely fixed at write time |
| `po_receipt_lines.cost_method_at_receipt` (`NOT NULL`) | method snapshot, sibling of the above | Keep |
| `inventory_adjustments.unit_cost` (`NOT NULL`, **header table**) | pool-derived, FIFO-reachable (`0035:202–210`, `_fifo_walk_layers` under `FOR UPDATE`) and lot-reachable (`0048`) | Rename to `observed_unit_cost`; authoritative cost via the view |
| `customer_return_lines.unit_cost` (`NOT NULL`) | **copied** from the ship line's snapshot (`0018:1476–1480`), not an independent pool read | Re-resolve through the view — a consumer (§3.3), not a rename site |
| `lot_transfer_lines.unit_cost` (nullable `NUMERIC(19,4)`) | pool-derived post-walk via `_lot_walk_layers` (`0057:874–880`) | Rename; **nullable**, so it can simply be left NULL until settlement without a schema change |
| `inventory_cost_adjustments.prior_unit_cost` / `pool_qty` (`NOT NULL`) | pool reads persisted on a document row (`0016:607`) | `wac_perpetual`-only today, so outside the FIFO/LIFO conflict — same class, watch during the port |
| `purchase_order_lines.unit_cost`, `po_return_lines.unit_cost`, `vendor_bill_lines.unit_cost`, `sales_order_lines.unit_price`, `customer_invoice_lines.unit_price` | **asserted**, not pool-derived | Untouched |

Two properties of that table size the job. First, the surface is **bounded and enumerable** — roughly
five pool-derived columns across three table shapes (document line, document header, adjustment
record) — not a pervasive convention. Second, the port action is **not uniform**: two are renames of
`NOT NULL` columns (schema change + wrapper edit + property-test rewrite), one is nullable and needs no
schema change at all, and one is a consumer that must re-resolve rather than be renamed.

### 4.3 Enforcement

`tests/property_ap9_audit_ledger_consistency.rs` is the model and needs a sibling, not a replacement.
It keeps enforcing the simultaneity form on `wac_perpetual` SKUs — and the parent's FIFO/lot paths need
that coverage regardless of this port, since no property test pins R7 on them today (§1.1). A new
property test enforces the convergence form on layer-tracked SKUs: random receive/deplete/backdate
sequences, drive recalc to quiesce, then assert (a) the view's `authoritative_unit_cost` equals an
independent layer walk, (b) the telescoping GL identity of §3.2 holds, and (c) **no document read at
status `final` ever changes afterwards**. Property (c) is the one that would have caught a two-state
flag.

## 5. The questions, and the arguments

All five were decided on 2026-08-07. This section keeps the arguments that produced the decisions;
**§7 is the decision record** and is the section to read for what was chosen.

**5.1 Does an invoice pin a generation, or float? → DECIDED: float, pin at close.**
Pinning at invoice time re-creates the drift R7 exists to prevent. Floating means an invoice's COGS is
not stable until close. What settled it is §1.2: floating is already what the parent does for
`wac_periodic` and `wac_retroactive`, where a shipped line's cost floats until the close hook posts
variance — so the decision extends an existing convention to three more methods rather than
introducing a new one. The consideration that could have gone the other way was an accounting or
contractual requirement to state COGS at invoice time, which would have outranked it and forced
5.2(c); no such requirement was asserted.

**5.2 What does a FIFO/LIFO document line show at write time? → DECIDED: (a), the observed cost,
labelled provisional.**
(a) the observed cost, explicitly labelled provisional; (b) nothing until settled — honest, and cheap
for the one nullable column but a schema change for the `NOT NULL` ones; (c) synchronously drain that
pool at document time, which is what the parent does today and which buys back premise 1 at the cost of
alt-C's hot-path posture for that call site. **(a) was chosen**, which is also the parent's
`wac_periodic` behaviour. Note one structural difference worth preserving: the parent needs
`posting_lines_provisional` as a **materialized worklist** so its close hook can find what to true up,
whereas v3.2 already has that worklist as the recalc queue plus settlement generations. A port should
not carry the side table across — it would be a second home for a fact the costing plane already owns.

**5.3 Should close write the final cost back onto the document line? → DECIDED: no.**
It would restore R7 literally post-close and give reporting a stable local join. Against: it adds a
write-amplification term to close (already the `soak-results.md` §4 storm axis, `acct-m0ab`), and it
duplicates a fact the view serves. **The view remains the authoritative read surface** — and the
parent agrees by precedent: its close hooks post variance and stamp `posting_lines_provisional`; they
do not rewrite `so_shipment_lines.unit_cost`.

**5.4 Should a base COGS leg be posted for FIFO/LIFO? → DECIDED: yes — option (c).**
Today generation 1 posts `(authoritative − observed) × qty` with `prior = observed` — exactly right
when the observed cost was journalled, and leaving the journal incomplete when it was not (§1.5). The
parent's convention is a complete journal at document time plus a later variance, so a document layer
almost certainly requires the base leg to exist somewhere. Three shapes:

- **(a) The seam posts it** when the cost becomes authoritative. Keeps v3.2's hot path untouched;
  puts journal completeness outside the costing plane.
- **(b) The engine posts it at generation 1** with `prior = 0` for a never-journalled depletion.
  **Not a one-line change**: the engine folds its net GL delta into `value_sum`
  (design-v3.2 §5.1 step 4) and the hot path has *already* reduced `value_sum` by the observed amount,
  so journal amount and aggregate fold must be computed separately or the pool double-counts. V5's
  structural assertion and V2's identity both encode the current shape and would need revising.
- **(c) The hot path posts a provisional cost leg at the observed cost**, exactly as the parent's
  `wac_periodic` path does. This makes `prior = observed` correct by construction — the delta model
  then needs no change at all — and it remains insert-only, so it does not reintroduce a
  read-modify-write or the serial-fold ceiling. It costs alt-C's "no GL on the hot path" property,
  which is a posture rather than a performance property, and it would need measuring before anyone
  claims it is free.

**(c) was adopted** — the closest fit to the parent's convention and the cheapest to reason about,
since it leaves the recalc delta model untouched. Implementation is `acct-zrju.7`; the measurement of
the hot-path cost rides that issue rather than gating it, and design-v3.2's posture line updates when
it ships.

**5.5 Resolved here: the flush-wipe divergence.** Not a document-plane concern (§3.3).

## 6. What this implies for Q2

`acct-476a.4` was filed as *"the parity item that decides whether alt-C can coexist with acct's
document-line conventions at all."*

The case for coexistence:

- **The parent already runs two-phase costing** for `wac_periodic` and `wac_retroactive`, with
  `so_ship` explicitly flagged provisional and trued up by a close hook (§1.2). The
  provisional/settled/final shape is not foreign to the document layer — it is already in it.
- The conflict reaches **three of the parent's seven cost methods** (`fifo`, `lot`, `lot_fifo`); two
  more are already close-settled, and only `standard` and `wac_perpetual` keep R7's simultaneity form.
- The authoritative read **already exists** as an engine query; the view generalizes it and introduces
  no second home for any fact.
- The snapshot surface is **bounded and enumerable** — roughly five pool-derived columns across line,
  header and adjustment tables — with non-uniform but small per-column port actions (§4.2).
- Finality is **stronger** than the parent's: closed-period cost is frozen by schema invariant with a
  prefix-stability argument behind it (§1.6).

What coexistence costs:

- **Mid-period COGS becomes a decision-support number for layer-tracked SKUs**, with a measured
  ~22–30% per-depletion error band and a directional bias. No read model removes that; it can only
  label it. This is a change to what the business sees, not merely to how the schema is shaped.
- **The return path (§3.3) and every affected wrapper's R7 property test** change shape — the tests
  from simultaneity assertions to convergence assertions.
- **The costing plane owes a base COGS leg.** §5.4 resolved this rather than leaving it hanging: the
  FIFO/LIFO hot path gains a provisional cost leg (`acct-zrju.7`). Until that ships, the as-built
  engine still has the no-base-leg shape of §1.5, and the journal for a FIFO/LIFO depletion carries
  only the recalc deltas.

**Verdict for this axis: asset, not rebuild.** With §5.4 decided, the verdict carries no open
condition. Nothing here argues for discarding the document layer: what it argues for is a bounded
migration of a small set of columns (§4.2), a re-resolved return path (§3.3), R7 property tests
restated from simultaneity to convergence (§4.3), and an explicit change to the mid-period COGS
contract. The document layer's semantics — its state machines, its document lifecycle, its posting
conventions — are untouched by anything in this note, and its existing provisional-cost machinery
(§1.2) is evidence that the conventions can absorb the change.

The view and status contract specified in §3 stands as written and does not wait on `acct-zrju.7`:
the base leg changes what the *journal* contains, not what the authoritative cost *is* or how a
document reads it. What it does change is §3.2's tie-out, which becomes a plain per-generation
reconciliation once the base is journalled — the telescoping identity is a statement about the
current shape, not a permanent one.

That verdict is scoped to the document-cost axis. What `acct-476a.2` conditions is the **aggregate Q2
call**: reservations are the sharper conflict, being a *synchronous gate* where alt-C deliberately
removed synchronous gating, and this note's method-scoping does not transfer there. Q2 should not be
called until both dossiers report.

## 7. Decisions (2026-08-07)

| # | Question | Decision |
|---|---|---|
| 5.1 | Invoice COGS: pin a generation, or float? | **Float until close, then pin.** Extends the parent's `posting_lines_provisional` convention to the layer-tracked methods rather than introducing a new one |
| 5.2 | What a FIFO/LIFO document line shows at write time | **The observed cost, labelled provisional.** The three-state status travels with the number wherever it is displayed |
| 5.3 | Does close write the final cost back onto document lines? | **No.** `document_cost` is the authoritative read surface; close posts variance and stamps, exactly as the parent's close hooks do — no document-line rewrite, no second home for the fact |
| 5.4 | Does anything post a base COGS leg for FIFO/LIFO? | **Yes — option (c).** The hot path gains a provisional cost leg at the observed cost, `wac_periodic`-style. Filed as `acct-zrju.7`; measurement of the hot-path cost rides the implementation |
| 5.5 | The flush-wipe divergence | Resolved in-note: not a document-plane concern (§3.3) |

The four substantive decisions are mutually reinforcing rather than independent: 5.4 makes the journal
complete at document time, which is what lets 5.1's float-then-pin sit on top of a real posting rather
than an absence; 5.2 supplies the label that makes floating honest to a reader; and 5.3 keeps the
correction flowing through variance postings, which is the only reason the labelled snapshot in 5.2
stays true after it is written.

**Consequences already recorded above:** §5.4's adoption removes the §6 verdict's condition; the
§3.2 tie-out is scoped as a statement about the pre-`zrju.7` shape; the §3 view and status contract
are unaffected either way.
