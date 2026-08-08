# Backport design note: reservations under alt-C's no-gate posture

> **Status: decided (`acct-476a.2`, 2026-08-07).** This note extends the spec-of-record
> [`design-v3.2.md`](design-v3.2.md); it does not amend it. It states the posture conflict between the
> `acct` document layer's reservation enforcement and v3.2's ratified no-gate posture, sets out the
> candidate shapes, and walks each against the axes the requirement owner actually raised.
>
> **§7 records the outcome: S3 (seam-side gate), with pinned reservations split out as a separate seam
> contract.** §1–§6 are the analysis that produced it and are preserved as written — §5.2's trade-off
> matrix stays unordered, per `acct-cz1v`'s process constraint ("sketch 2-3 alternative shapes — *not*
> a single recommended shape — and walk each one against the existing flows"), because the matrix is
> the record of what was weighed, not an argument for the answer.
>
> Per convergence Q6 this dossier and its sibling
> [`design-v3.2-backport-document-cost.md`](design-v3.2-backport-document-cost.md) (`acct-476a.4`) run
> immediately after the spec-of-record, because together they decide **Q2** — whether the document
> layer is ported onto v3.2 or rebuilt against it. §6 states what reservations contribute to that
> judgment. Where this note describes when a cost is knowable it uses the sibling's three-state
> vocabulary (`provisional` → `settled` → `final`) unchanged.
>
> **Scope fold.** This note absorbs `acct-cz1v` (reservation architectural exploration, closed
> merged-into-`acct-476a.2` per Q6). The **five** axes in §4 are that issue's — note that
> `acct-476a.2`'s scope-fold note lists only four, omitting the WO-vs-SO allocation target, which §4.5
> restores. The quotes in §4 are the requirement owner's own words from 2026-05-12, preserved verbatim.
> They are the doubt this note is required to carry, not a problem statement invented here.

## 1. The conflict, stated precisely

The parent system enforces reservations **synchronously, at the moment of promise, by reading pool
state under a row lock**. The live `reserve_inventory`
(`db/migrations/0052_lot_reserve_inventory.up.sql:92`) takes `FOR UPDATE` on the `stock_available`
row(s) for the `(sku, location)` and computes a promisable bound in one of two branches:

- **Generic** (`0052:188-200`): `Σ over open stock_available rows (debits_total − credits_total)`
  minus `Σ qty of reservations WHERE status = 'active'`.
- **Pinned** (`0052:152-166`): `_inventory_lot_remaining_qty(lot_id, receipt_date)` minus the active
  reservations that could draw on that lot (`lot_specific AND lot_id = p_lot_id`, **or** not pinned).

Either way it returns `NULL` — refusing the promise — when the request exceeds the bound
(`0052:203-205`). The predecessor's header explains why this is a function and not a CTE: in
`READ COMMITTED` the post-lock `SELECT` sees the prior winner's `INSERT`, so concurrent reservers
serialize correctly. That is a deliberate, correct, synchronous per-pool gate.

v3.2 removed exactly that shape. design-v3.1 §16 decided alternative C: the hot path records the
physical event with **"no synchronous read, no qty gate, and no running-average maintenance as a
correctness dependency"**, and quantity becomes "a running signal: a depletion beyond on-hand drives
the aggregate negative and is *flagged*, not rejected". `pool_state.qty` carries no non-negativity
constraint, and the schema says so in as many words
(`poc/ledger-v3.2/db/migrations/0003_ledger_tables.up.sql:35-39`). The stated reason is not
squeamishness about gates: §16 records that the synchronous per-pool gate was the *only* reason the
hot path read `pool_state` at all, and that **any** posture retaining such a gate keeps the per-pool
serial-fold ceiling — explicitly including gates that are cheap
(`poc/design_research/design-v3.1.md:1296-1301`).

So the conflict is not "does the ledger allow negative inventory". It is narrower and sharper:

> **A reservation is a promise about future quantity. Enforcing it requires reading current quantity
> at promise time. Alt C removed the ledger's synchronous quantity read. Where does the read go?**

Two clarifications keep the scope honest.

**This is not the WAC deviation.** design-v3.2 §9 records that the strict methods still gate quantity
synchronously, and that convergence Q3b directs that toward removal. Reservations are a *different*
gate at a *different* moment — promise time, not depletion time — and would remain a question even if
the WAC gate were removed tomorrow. Do not resolve them together.

**There is a second coupling, and it is not about quantity at all.** A pinned reservation's `lot_id`
selects which cost layer a shipment consumes (§2.1, §4.3, §6). That coupling is orthogonal to the
quantity-gate question and is *not* resolved by any of §3's shapes. It is the finding with the
largest effect on §6's Q2 judgment.

## 2. What exists on each side today

### 2.1 Parent (`acct`) — current-state audit

Function citations are to the **latest** `CREATE OR REPLACE` (`0052` / `0053` / `0068` / `0067`);
schema citations are to the defining migration where no later one alters it.

| Element | Where | Note |
|---|---|---|
| `inventory_reservations` | `db/migrations/0011_inventory_reservations.up.sql:10-23` | First-class rows, per `CLAUDE.md:112` (B1: not self-pending postings) |
| `reservation_status` enum | `db/migrations/0002_types_and_enums.up.sql:197-203` | `active`, `allocated`, `shipped`, `cancelled`, `expired` |
| Promise-time gate | `0052_lot_reserve_inventory.up.sql:92` | Two branches (§1); returns `NULL` on refusal, does not raise. `P0052` when `lot_specific` without `lot_id` (`0052:116`) |
| `active → allocated` | `0053_lot_so_allocate_ship_pinned.up.sql:66` | Sweep at `:146-149`, **preceded by** the W1 pinned-lot residual validation `:97-131` (`P0053`) |
| `→ shipped` | `0068_wrapper_instrumentation.up.sql:43` | Flip at `:469-473`, accepting `status IN ('active','allocated')` |
| Pin resolution at ship | `0068:308-313`, `:322-326`, `:336-339` | Reads pinned reservations to pick the lot; `P0055` ambiguous, `P0054` caller/pin conflict |
| `→ expired` | `0011`, `pg_cron` every 30s | Sweeps `active` rows past `expires_at` |
| `→ cancelled` | **nowhere** | **Unreachable dead state** — no wrapper, hook, or job sets it; it exists only in the enum and a comment (`0011:8`) |
| Lot pinning | `0044_lot_schema.up.sql:235-244` | `lot_id` + `lot_specific` discriminator + CHECK + partial index |
| Unit pinning | `0061_lot_serial_schema.up.sql:254-266` | `unit_ids BIGINT[]`, GIN index; `NULL` = any-units, stated in a COMMENT only |
| `unit_price` | `0011:21` | Written by `reserve_inventory`, **read by no function** in `db/migrations/` — `post_so_ship` takes its price from the SO line (`0068:167`) |
| Over-reservation recon | `0067_partition_registry.up.sql:280-301` | Daily alert: on-hand < Σ reserved, filtering `status = 'active'` (`:294`) |

**Read-site classification.** Every read of `inventory_reservations` falls into three kinds, and the
filter split between them is what produces §4.1's finding:

| Read site | Filter | What it cares about |
|---|---|---|
| `reserve_inventory` promisable (`0052:159-166`, `:190-195`) | `status = 'active'` | Quantity held against future promises |
| Over-promise recon (`0067:294`) | `status = 'active'` | Quantity held, for alerting |
| Ship-time pin resolution (`0068:308-313`, `:327-333`) | `status IN ('active','allocated')` | **Which cost layer to consume** |

The first two see only `active`; the third sees both. That asymmetry is load-bearing and, on the
evidence below, looks unintended.

### 2.2 v3.2

There is **no reservation table, lifecycle, or concept** in v3.2, and none was charter-promised. The
relevant surfaces are `pool` (`0003_ledger_tables.up.sql:7-20`), keyed
`(sku_id, location_id, identity_key)` with `identity_key != 0` reserved for specific-id pools
(`0003:11,18-19`), and `pool_state` (`0003:45-53`), whose `layer_id = 0` row is the aggregate quantity
signal. Layer identity and FIFO/LIFO layer assignment belong to the recalc engine, not to any caller.

### 2.3 The flag does not exist — and already has a home

design-v3.1 §16 and design-v3.2 §3 both say a beyond-on-hand depletion is "flagged, not rejected".
**The rejection was removed; the flag was never built.** In the v3.2 tree: the three shipped gauge
views are `feed_lag`, `recalc_queue_depth`, and `recalc_pool_lag`
(`poc/ledger-v3.2/db/migrations/0014_feed_gauges.up.sql:10,21,32`, the last replaced by
`0015_recalc_pool_lag_physical.up.sql:8`), none with a quantity-sign predicate; the conservation
sweep's V1 check is *conservation* — `aggregate qty == Σ trx_line.qty per pool`
(`poc/ledger-v3.2/ledger-bench/src/verify.rs:8`) — which a uniformly negative pool passes; and no
`qty < 0` predicate exists anywhere in the migration tree.

This is **not** a first discovery. `acct-gtp7.3` (open, filed 2026-07-12) states it verbatim: *"The
§16 'flagged, not rejected' negative-inventory posture has NO flag surface: no view lists pools with
aggregate qty < 0, no negatives count in the close report"*, and its `What` proposes exactly the
missing pieces (a `negative_pools` view, V1 as a view, a drift bound, method-scoped
`recalc_pool_lag`). This note records the gap because every shape in §3 depends on the flag being
real, and points at `acct-gtp7.3` as its existing home.

## 3. Candidate shapes

Three, described by where the promise-time quantity read happens. They are set out in §5.2 as an
unordered matrix; the ordering here is expository only.

### S1 — Ledger-side admission consult (the `0018` probe pattern)

`acct-476a.2` names this: reservations become ledger-side state, consulted at admission, following the
pattern migration `0018` proved for backpressure.

That pattern's mechanics matter, because its cheapness is the whole argument. `recalc_backpressure` is
a normally-empty table keyed by `pool_id`
(`poc/ledger-v3.2/db/migrations/0018_backpressure.up.sql:61-66`). The staging-path gate is a
`BEFORE INSERT` trigger on `ledger_inbox` whose first statement is
`IF NOT EXISTS (SELECT 1 FROM recalc_backpressure) THEN RETURN NEW;` (`0018:81-83`) — one lookup
against an empty index in the common case. Rejection raises `53400`. Two design details carry over:
malformed payloads **fail open** (`0018:91-93`), because the gate is admission control and not
validation; and admission is the *only* gate — "accepted work is never retroactively failed"
(`0018:6-9`).

**Where it strains.** The analogy holds on mechanism but breaks on occupancy. Backpressure's probe is
cheap *because the table is normally empty*; reservations are **normally present**, so the empty-table
fast path never fires and the consult becomes a real per-pool read at admission. And the ceiling
argument does not rescue it: design-v3.1 §16 (`:1296-1301`) considered precisely a cheap, cost-free
quantity gate — SPIKE-B's bare `UPDATE … WHERE qty − Δ >= 0`, no `pool_lock`, no running-average
maintenance — and ruled that **"any posture that retains a synchronous per-pool qty gate keeps the
ceiling"**. S1 therefore reintroduces the per-pool serial-fold ceiling by the ratified decision's own
terms. Its only distinction from the v3.1 gate is that it need not read `pool_state`'s cost columns,
which §16 explicitly identifies as not the cost driver.

**Where it gains, and this is new.** If reservations live in the ledger, a pinned reservation naming a
cost layer is at least in the plane that owns layers (§2.2). S1 is the only shape for which the
lot-pinning coupling (§4.3, §6) is not a cross-plane violation.

### S2 — Aggregate-signal soft reservations

Reservations are recorded but never gate. Reserved quantity becomes a second running signal alongside
on-hand: promise-time checks read it advisorily, breaches surface through the same flagging and
reconciliation machinery as negative quantity, and no write path is ever refused on reservation
grounds.

This is alt C's own move — turn a gate into a signal — applied one level up. It is also close to what
the parent already does in one respect: the over-promise recon check (`0067:280-301`) exists because
the synchronous gate is not trusted to be sufficient on its own.

**Where it strains.** It is a business-behaviour change, not an internal one. Today
`reserve_inventory` returning `NULL` is how a sales order learns it cannot promise stock. Under S2 the
promise always succeeds and the breach surfaces later. Whether that is acceptable is a policy question
about whether this system may promise inventory it does not have, and it cannot be settled inside this
dossier.

### S3 — Seam-side gate (document layer keeps enforcement)

The ledger learns nothing about reservations. The document layer keeps `inventory_reservations` and
its promise-time gate, reading whatever quantity view it needs; the ledger stays a pure recorder.

**Provenance and supporting input.** This shape is not in the `acct-476a.2` body. It comes from
design-v3.2 §9's Q3b paragraph — *"Gating is a document/seam concern, not a ledger concern"* — which
is a ratified statement about this class of question, and from the observation that
`reserve_inventory` is already a document-layer function that happens to read ledger-owned state. That
is one input to weigh, not a decision.

**Where it strains.** It relocates the problem, and the relocation has two costs. First, the document
layer must define which quantity it reads and how current that read must be. The aggregate is exact
under conservation (V1) modulo **un-drained `ledger_inbox` envelopes** (admitted but not yet applied,
`0018:6-9`) and in-flight uncommitted direct transactions — *not* modulo feed lag: the logical-decoding
feed writes recost floors, backlog counters and dirty marks only
(`ledger-feed/src/consumer.rs:252,287-294,308`) and never touches `pool_state.qty`, so `feed_lag`/G1 is
the wrong gauge for a quantity reader to watch. Second, and larger: "the table ports unchanged" is true
only for generic reservations. A pinned reservation is a document-layer row that dictates a
costing-plane decision (§4.3), which is exactly the cross-plane coupling a seam is supposed to forbid.

## 4. The five axes

These are `acct-cz1v`'s, and they are logically prior to §3: they ask what a reservation *is*, and
every shape above assumes an answer. The requirement owner's framing, verbatim:

> *"I have doubts about our current reservation system (architecturally frail), and would probably
> want to rework how statuses, active vs inactive work, and treat allocations for work orders or
> sales orders separately from status."*

> *"I am not arguing that reservations would be derived, only that they are separate from status.
> They can be soft or hard, be detailed (lot/inv #) or generic. Shipped vs not shipped is very
> relevant — you are decreasing inventory (any active vs non-active status has cost implications like
> a quantity change)."*

**No shape is proposed for any axis.** Each subsection records the current state, the owner's doubt,
and what each candidate shape would have to answer.

### 4.1 Status decomposition

`reservation_status` (`0002:197-203`) carries four jobs at once, as `acct-cz1v` enumerates: whether the
reservation is in effect; which document drove the latest transition; committed-pre-ship (`allocated`)
versus tentative (`active`); and the terminal state where inventory actually moved (`shipped`). A
fifth value, `cancelled`, has no writer at all (§2.1) — the enum is wider than the lifecycle.

The live code refines the owner's framing rather than simply confirming it. The `active`/`allocated`
distinction is **not** a shipping precondition: `post_so_ship` accepts both (`0068:469-473`), and no
`WHERE` clause anywhere filters on `allocated` alone. But it is **not inert either**, and it acts in
the opposite direction from a commitment:

- Flipping `active → allocated` **releases the promise-time hold.** Both `reserve_inventory` branches
  subtract only `status = 'active'` (`0052:159-166`, `:190-195`), so the same on-hand becomes
  promisable again to a later reservation the moment an SO is allocated.
- It also **drops the row out of the over-promise alert**, which has the same active-only filter
  (`0067:294`) — so an allocated hold is invisible to the parent's own safety net.
- Entering `allocated` is nonetheless a **validated** transition, not a label flip: `post_so_allocate`
  runs a W1 loop over every pinned active reservation for the SO, takes `FOR UPDATE` on the matching
  `stock_available` rows, reads `_inventory_lot_remaining_qty`, and raises `P0053`
  `pinned_lot_underfulfilled` if the pinned lot cannot cover it (`0053:97-131`), aborting the whole
  allocation.

So the axis is real, but the defect is sharper than "it's just a label": a status value that reads as
*more* committed makes the hold *less* enforced. That is direct evidence for §4.2.

**Shape interaction.** S1 must decide which states count as held, so it forces at least a partial
decomposition. S2 and S3 *can* carry today's enum — but doing so ports the release-on-allocate
behaviour unexamined, which is a decision, not a deferral.

> **Decided (D-R3).** Today's schema carries across unchanged and the decomposition axes are deferred.
> The release-on-allocate behaviour is **recorded, not fixed**: the owner chose record-not-fix
> explicitly, so it ports as-is and its correction is port-time work. It is documented here precisely
> so the port does not carry it unexamined — which was the objection to deferring it.

### 4.2 Soft versus hard

`acct-cz1v` defines these precisely: soft = displaceable intent, expirable; hard = commitment,
untouchable until the owning document releases it. Today `active`/`allocated` "loosely reflects this
distinction but the soft/hard semantics aren't first-class" — and §4.1 shows the loose encoding
inverts, releasing the hold at the moment of commitment.

**The dependency is asymmetric.** S1's gate is only coherent if a hard class exists — a gate that
refuses a depletion on account of a *displaceable* hold is wrong. S2 is coherent if all reservations
are soft and is **eliminated** if any must be hard (a hard reservation that never gates anything is
not a commitment). S3 is coherent under **either** answer: it defers the distinction to the seam,
where it must still be answered but does not gate the shape choice.

So this axis discriminates S1 from S2; it does not block choosing S3.

> **Decided (D-R1): resolved by construction.** S3 was chosen, so soft-versus-hard is not answered
> here — it defers to the seam, and the parent's existing semantics carry over with it. The parent's
> promise-time gate is *hard* in effect (`reserve_inventory` refuses), and that behaviour is preserved
> unchanged; making the soft/hard split first-class remains available as future seam work.

### 4.3 Detailed versus generic

Generic = qty at `(sku, location)`. Detailed = specific `lot_id`, `unit_ids[]`, serial.

The current state is more modelled than `acct-cz1v`'s "bolted on" framing suggests, and the difference
matters. **Lot-level detail is explicitly modelled and enforced**: a `lot_specific` discriminator with
a CHECK tying it to `lot_id` (`0044:235-244`), a promisable branch of its own (`0052:152-166`), a
validated allocation transition (`P0053`, `0053:97-131`), and pin-resolution guards at ship (`P0055`
ambiguous, `P0054` conflict — `0068:322-339`). That is a four-code lifecycle, not a convention.
**Unit-level detail is not**: `unit_ids` states its discriminator convention (`NULL` = any-units) in a
COMMENT (`0061:254-266`) and is currently written but never read by any wrapper.

One structural observation, offered as an observation and not a proposal: v3.2's `pool` is keyed
`(sku_id, location_id, identity_key)` with `identity_key != 0` reserved for specific-id pools
(`0003:11,18-19`), so the generic/detailed split already has *a* representation in ledger pool
identity. Whether reservations should ride it is precisely what this note must not pre-answer.

**Shape interaction — no shape can defer this.** A lot pin is not a filing detail: `post_so_ship`
resolves the consumed lot from the reservation (`0068:308-313`) and passes it into
`_lot_walk_layers(...)` (`0068:346-349`), whose result sets `v_unit_cost` (`0068:354`) — the COGS leg
amount and the `so_shipment_lines.unit_cost` / `lot_id` audit snapshot (`0068:388-394`). The
reservation row therefore **selects which cost layer is consumed**. Under S1 that selector sits in the
plane that owns layers; under S2 and S3 it sits in the document layer while v3.2 assigns FIFO/LIFO
layers in the engine — a conflict each must resolve explicitly.

> **Decided (D-R2a): pinned reservations split out.** S3 was chosen for quantity, and the conflict
> this paragraph names is resolved by not pretending the pin is a quantity concern. A lot pin becomes
> an **input the seam passes to the costing plane** — a separate seam contract, specified alongside
> `acct-476a.3`'s seam work. Generic reservations port behind the seam untouched; the pinned path is
> new contract work, not a port. Unit-level pinning (`unit_ids`) is unaffected for now, being
> write-only today.

### 4.4 Shipped-as-cost-relevant

The owner's point is that `active → shipped` is not a label change: *"you are decreasing inventory
(any active vs non-active status has cost implications like a quantity change)"*. Today `shipped` sits
in the same enum as `expired` and the unreachable `cancelled`, which obscures that one of them
accompanied a real cost event and the others did not.

The code shows the coupling exists in control flow but not in the data model: `post_so_ship` performs
the reservation flip in the same function that posts the shipment (`0068:469-473`).

**Shape interaction, asymmetric.** Under alt C the cost accompanying a shipment is *not final at ship
time*. In the sibling dossier's vocabulary, a FIFO/LIFO shipment line is `provisional` at ship (no
`cost_settlement` row); it becomes `settled` after the first recalc pass and is **still revisable** (a
backdated event behind it forces a new generation under R-2, and it may carry `recost_pending`); it is
`final` only once its `posted_at` falls in a closed period. So a reservation model that treats
"shipped" as the moment cost is known imports an assumption v3.2 does not honour. Whatever `shipped`
means, **it cannot mean "cost is final"**.

> **Decided (D-R4): shared vocabulary adopted.** `shipped` is a quantity/lifecycle fact, never a cost
> assertion; the three states `provisional` → `settled` (revisable) → `final` are the shared vocabulary
> across both dossiers. One update from the sibling's decision round sharpens this: `acct-476a.4` §5.4
> resolved as option (c) — the FIFO/LIFO hot path will post a **provisional cost leg at the observed
> cost** (`acct-zrju.7`). A shipped line therefore has a journalled base amount from the moment it
> ships, rather than no cost leg at all, and later recalc posts variance against it. That does not make
> `shipped` mean final — the base is `provisional` by construction — but it does mean the seam has a
> real journalled figure to reconcile against instead of a gap.

### 4.5 WO-versus-SO allocation target

The owner's headline quote names this axis — *"treat allocations for **work orders** or sales orders
separately from status"* — and `acct-cz1v` develops it:

> *"WO allocations and SO allocations both end up as 'allocated' status today but mean different
> things: WO: raw inventory committed to be consumed at WO start / op_arrival; SO: FG inventory
> committed to be shipped out. What would change if allocation-target (WO vs SO vs other) were
> explicit, and the lifecycle rules diverged where their semantics actually diverge?"*

**The current state refines the premise.** `inventory_reservations.so_id` is
`UUID NOT NULL REFERENCES sales_orders(id)` (`0011:15`), and no later migration alters it — only
`0044` and `0061` touch the table, adding lot and unit pinning. Work-order allocations therefore
**cannot exist as reservation rows at all**: the schema is structurally SO-only, so there is no
existing conflation to decompose. The axis is a forward-looking schema question — whether to add an
allocation-target discriminator and relax `so_id`, and whether WO and SO lifecycles should diverge —
rather than a cleanup of something already mixed.

**Shape interaction.** Largely orthogonal to §3: all three shapes would carry the same discriminator
question. It bears on §4.2 though, because WO material commitments and SO promises are the most likely
place a genuine soft/hard split would first be needed.

> **Decided (D-R5): forward-looking only.** Not in scope for the port. `inventory_reservations` stays
> structurally SO-only (`so_id UUID NOT NULL`), because there is no existing WO allocation to
> decompose — the axis is recorded here as a schema question for whenever WO material commitments are
> wanted as reservation rows.

## 5. Trade-offs and decisions

### 5.1 An observation about the flag

Every shape here degrades to "the breach is invisible" unless the negative-quantity flag surface
(§2.3) exists, and the posture design-v3.1 §16 ratified assumes it does. `acct-gtp7.3` already carries
that work. Whether to prioritise it alongside this decision is the owner's call; this note records the
dependency rather than prescribing the sequencing.

### 5.2 Trade-off matrix — unordered

Per `acct-cz1v` deliverable 3, presented as alternatives, not a recommendation.

| | **S1** ledger-side consult | **S2** aggregate signal | **S3** seam-side gate |
|---|---|---|---|
| Promise-time read | Ledger, at admission | None (advisory only) | Document layer |
| Serial-fold ceiling (§16) | **Reintroduced** — §16 rules any synchronous per-pool qty gate keeps it | Not reintroduced | Not reintroduced in the ledger |
| Hot-path cost | Real per-pool read; the `0018` empty-table fast path never fires | None | None |
| Requires D-R1 (soft/hard) first | Yes — gate incoherent without a hard class | Yes — eliminated if any hold must be hard | **No** — defers to the seam |
| Business behaviour | Unchanged (promises still refused) | **Changed** — promises never refused | Unchanged |
| Lot pin (§4.3) as cost selector | Coherent — selector sits in the layer-owning plane | Cross-plane: document row selects engine layer | Cross-plane: same |
| Change to v3.2 | Largest — adds an admission-time consult the architecture was designed without | Small — new signal, no gate | None in the ledger |
| Ratified-decision fit | Against §16's ceiling finding | With alt C's own logic | With Q3b's *"gating is a document/seam concern"* |
| Release-on-allocate (§4.1) | Must be resolved (defines held states) | Portable as-is, but unexamined | Portable as-is, but unexamined |

**One observation the evidence forces, offered as an observation.** The quantity axis and the
cost-layer axis do not point the same way: S3 is the best fit for *where the quantity gate lives*, and
S1 is the only coherent home for *lot pinning as a cost-layer selector*. A single shape may therefore
not be the right unit of decision — generic and pinned reservations may want different answers. This
is derived from the pin's role in `_lot_walk_layers` (§4.3), not proposed as a fourth shape.

> **This observation is what the decision took up.** §7 records S3 for quantity with the pinned path
> split out as separate seam work — the two axes answered separately rather than forced into one shape.

### 5.3 Decisions required

> **All resolved 2026-08-07 — see §7 for the outcomes.** The questions are preserved as posed, so the
> decisions in §7 can be read against what was actually asked.

- **D-R1 (soft/hard, §4.2).** Are hard reservations required — commitments the system must be unable
  to violate — or is every reservation displaceable intent? **Blocking for S1 and S2 only**; S3 defers
  it to the seam by construction.
- **D-R2 (shape, §3/§5.2).** S1, S2, or S3? **Answerable before D-R1 if the answer is S3**; an answer
  of S1 or S2 requires D-R1 first.
- **D-R2a (§5.2 observation).** Should the decision be a single shape, or may pinned and generic
  reservations take different shapes given the pin's cost-plane role?
- **D-R3 (status, §4.1; detail, §4.3).** Does the release-on-allocate behaviour get fixed as part of
  the port, or carried across? Note that "carry today's schema" is not a neutral deferral — it ports a
  status value that weakens the hold it appears to strengthen. Lot-level detail cannot be deferred at
  all (§4.3); unit-level detail can, being currently write-only.
- **D-R4 (§4.4, cross-dossier).** What does `shipped` mean when a shipment's cost is `provisional` at
  ship, `settled` but revisable after recalc, and `final` only at close? Must be answered in the
  sibling dossier's vocabulary.
- **D-R5 (§4.5).** Is the WO-vs-SO allocation target in scope for the port — i.e. does
  `inventory_reservations` gain an allocation-target discriminator and lose `so_id NOT NULL` — or does
  it stay SO-only?
- **D-R6 (P0006-vs-flag mapping).** The parent enforces non-negative inventory through the `accounts`
  CHECK (`23514`) and raises `P0006` on empty-pool depletion for the WAC family (`acct-9ij`;
  `CLAUDE.md`). Under alt C those conditions are flagged, not raised, for FIFO/LIFO. Each parent
  rejection must map to: retained (strict methods only), relocated (seam-side pre-check), or converted
  to a flag. `acct-9ij` is the natural home for the enumeration; it should not be re-litigated here.

Cross-reference, not a decision: the negative-quantity flag surface is `acct-gtp7.3` (§5.1).

## 6. What this contributes to Q2 (asset-versus-rebuild)

**Reservations are entangled with the parent's costing plane — through lot pinning, not through
cost columns.** The table holds no cost: its only monetary column, `unit_price` (`0011:21`), is a
sales price and is written-never-read (`post_so_ship` takes price from the SO line, `0068:167`). But
`lot_id` / `lot_specific` are **cost-layer selectors**. `post_so_ship` resolves the consumed lot from
the pin (`0068:308-313`, guards at `:322-339`), feeds it to `_lot_walk_layers` (`0068:346-349`), and
derives the depletion's unit cost (`0068:354`) — which becomes the COGS amount and the
`so_shipment_lines` audit snapshot (`0068:388-394`). Two further couplings run the same way:
`reserve_inventory` bounds pinned promisable by the lot subledger residual (`0052:152-166`), and
`post_so_allocate` re-validates it (`P0053`, `0053:97-131`).

This changes the Q2 picture in three ways.

**The generic path still ports cleanly.** A non-pinned reservation is pure quantity state with no
cost coupling, and under S2 or S3 it ports essentially unchanged. The B1 decision that made
reservations first-class rows (`CLAUDE.md:112`) holds up well against v3.2.

**The pinned path does not.** Porting `lot_id` / `lot_specific` ports a cost-layer selector into a
system where layer identity is `pool.identity_key` and FIFO/LIFO layer assignment belongs to the
recalc engine (§2.2). That is a seam question in its own right — *may a document row name a cost
layer, and if so, how is that expressed to an engine that assigns layers itself?* — and it must be
answered before any port, under every shape. It is the strongest "not a pure asset" signal this
dossier found.

**The shape choice may itself change v3.2.** If hard reservations are required and the guarantee must
be the ledger's (S1), v3.2 gains a synchronous admission-time consult it was explicitly designed
without, and — per §16's own reasoning — takes back the per-pool serial-fold ceiling. That is a change
to the *surviving architecture*, not to the document layer, and it is a materially different Q2
conversation than S2 or S3 produce. The asset-versus-rebuild judgment should not be finalized before
D-R1, D-R2, and D-R2a are answered.

### 6.1 Q2 verdict for this axis — decided

**No rebuild forced.** S3 was chosen, so the branch above that would have changed v3.2 is not taken:
the ledger acquires no reservation concept, no admission-time consult, and no per-pool serial-fold
ceiling. `inventory_reservations` and `reserve_inventory` port behind the seam as the asset they are,
carrying their existing hard-in-effect semantics.

The one piece of genuinely new work — expressing a lot pin to the costing plane — is **seam contract
work, not a rebuild trigger**. It does not invalidate the parent's reservation model; it defines how a
document-layer pin is communicated to a plane that assigns layers itself. Reservations therefore
report **asset, not rebuild**, for the Q2 judgment.

## 7. Decisions (2026-08-07)

Recorded in the decision round on this dossier. `acct-476a.2`'s question — where the promise-time
quantity read goes under alt C — is answered; the axes it folded in from `acct-cz1v` are answered or
explicitly deferred with their reasons.

| ID | Decision |
|---|---|
| **D-R2 / D-R2a** | **S3, split pinned/generic.** Quantity gating stays at the document/seam. `reserve_inventory` carries over with its hard semantics; the ledger is unchanged. Pinned (lot-specific) reservations are a **separate seam contract** — the pin becomes an input the seam passes to the costing plane, specified alongside `acct-476a.3` |
| **D-R1** | **Resolved by construction.** Under S3, soft-versus-hard defers to the seam; parent semantics carry over unchanged |
| **D-R3** | **Carry today's schema.** Decomposition axes deferred. The release-on-allocate behaviour (§4.1) is **recorded as-is, fix deferred to port time** — record-not-fix was chosen explicitly |
| **D-R4** | **Shared vocabulary adopted.** `shipped` ≠ cost-final; `provisional` → `settled` (revisable) → `final`. `acct-476a.4` §5.4 resolved as option (c), so a shipped line now carries a journalled provisional cost leg at observed cost (`acct-zrju.7`) for the seam to reconcile against |
| **D-R5** | **Forward-looking only.** Reservations stay structurally SO-only; the WO-vs-SO discriminator is recorded, not scoped |
| **D-R6** | **Home is `acct-9ij`.** The P0006-vs-flag enumeration lives there; a note has been appended to that issue |
| Q2 (§6.1) | **No rebuild forced** — asset, behind the seam; the pin contract is new seam work |

**What is now open work rather than open question:** the pinned-reservation seam contract (with
`acct-476a.3`), the provisional cost leg (`acct-zrju.7`), the negative-quantity flag surface
(`acct-gtp7.3`, §5.1), and the port-time correction of release-on-allocate. None of these blocks the
Q2 judgment.
