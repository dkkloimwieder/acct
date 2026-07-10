# design-v3.2 recalc engine (d): state / bookkeeping schema + idempotent re-run

> **Status: DESIGN (acct-q1oj.4, 2026-07-10).** The schema-and-idempotency child of the v3.2 recalc/close
> workstream (acct-q1oj / `design-v3.2.md` §5). Provides the persistent state the layer-walk (child (a),
> `design-v3.2-recalc-a.md`) requires, and the re-run contract that lets a recalc pass be safely repeated
> (crash-recovery, backdated re-cost, forced re-close). Resolves the two seams (a) handed here — **D1**
> (`posted_at` access path) and **D3** (per-consumed-layer linkage form). **Design-only:** DDL sketch +
> idempotency proof obligation; no migrations, no code.

## 1. Scope and the fixed constraint

Child (a) established the algorithm: a per-pool strict layer-walk in `(pool_id, posted_at, id)` order (R-1),
replayed from a re-cost floor when a backdated receipt lands (R-2), producing per depletion an authoritative
unit_cost + a variance-vs-provisional GL leg + per-consumed-layer linkage, and materializing the pool's
layer state. This child designs **where all of that lives** and **how a re-run stays correct**.

**The one fixed constraint (design-v3.1 §17, do not re-open):** the durable *stream* cursor is the
logical-decoding slot's `confirmed_flush_lsn` — recalc does **not** add a home-grown watermark / settled-state
scan column to advance over `trx_line`. Everything below is *pool-side authoritative-state* bookkeeping, which
is a different thing from stream progress: the slot says "what has been delivered in commit order"; the
tables below say "what has been authoritatively costed, and to which recalc generation." Keeping the two
distinct is the whole point of §17.

**Current shipped baseline (v3.1 migrations 0006–0011) this builds on:**
- `pool_state(pool_id, layer_id, qty, unit_cost, value_sum, …)`. `layer_id = 0` is the aggregate; `layer_id > 0`
  is a materialized layer, `layer_id = ` the receipt `trx_line.id` (§2.2). The `qty >= 0` CHECK (0006) and the
  dropped `value_sum >= 0` CHECK (0009) are **both scoped to `layer_id = 0`**, so **layer rows are already
  unconstrained** — recalc can write / rebuild them freely. `value_sum` (0007) is the net posted book value,
  reconcilable to Σ posting_line amounts, and is *allowed to go negative* on the aggregate under provisional
  standard-basis depletions (0009) until recalc trues it up.
- `trx_line(id, trx_id, pool_id, line_type, source_id, qty, unit_cost, source_trx_line_id, …)`; index
  `trx_line_pool_id (pool_id, id)` (0010); CHECKs `qty <> 0`, `unit_cost >= 0` (0011). `source_trx_line_id` is
  a **single** self-reference — the D3 gap (§4). `posted_at` is **not** denormalized (lives on `trx`) — the D1
  gap (§3).

## 2. Enum + sequence additions (the §7 surface)

Recalc posts its authoritative valuation as ordinary ledger rows, so it needs its own transaction taxonomy:

- **`trx_type += 'cost_adjustment'`** — the system-generated recalc trx wrapping a pass's cost corrections.
- **`line_type += 'cost_adjustment_line'`** — the per-depletion correction line.
- **`posting_event_type += 'cost_adjustment'`** — a **distinct** event type, *not* the existing `'variance'`.
  Rationale: `'variance'` already carries the hot-path STD purchase-price variance (§3.3); tagging recalc's
  FIFO/LIFO cost correction with its own event type keeps the two auditably separable (a report can answer
  "how much of GL movement is deferred-recalc correction" without disentangling PPV). One extra enum label is
  cheap; conflating them is lossy.
- **`cost_adjustment_id_seq`** — a sequence supplying `trx.source_id` for adjustment trxs. Adjustment trxs are
  system-generated and still must satisfy the `UNIQUE (trx_type, source_id)` idempotency key (§2.2); a
  dedicated sequence gives each pass a collision-free `source_id` and makes `(cost_adjustment, seq)` the
  natural idempotency handle for the pass itself.

## 3. D1 — `posted_at` access path (from child (a))

Recalc scans each pool's whole stream in `(posted_at, id)` order, repeatedly (R-2 replays it). Two ways to get
that order; (a) recommended and this child adopts:

> **D1 resolved: denormalize `trx_line.posted_at` and index `(pool_id, posted_at, id)`.**
> `ALTER TABLE trx_line ADD COLUMN posted_at TIMESTAMPTZ` (populated from `trx.posted_at` at insert time —
> the hot path already holds it, so this is one extra constant-time column write, off the locking critical
> section, exactly like the `posting_account_map` hydration §3.7). Add
> `CREATE INDEX trx_line_pool_posted ON trx_line (pool_id, posted_at, id)`. Whether the 0010 `(pool_id, id)`
> index is retained or dropped depends on whether any non-recalc lookup still needs pure id-order within a
> pool; recalc itself only needs the `(pool_id, posted_at, id)` path, so the composite can *replace* 0010's
> index if no other consumer needs it (mirror 0010's own "leading prefix subsumes" reasoning).

This turns the R-1 ordered walk into an index-only scan and removes a `trx JOIN` from recalc's hot inner loop.
It is a **hot-path write-contract addition** (one more `trx_line` column written) — surface to design-v3.2 §3
alongside the alt-C append shape. Note the companion **write-path obligation** (a) already flagged: the
populated `posted_at` MUST be the real business/effective date, not a wall-clock stamp (the v3.1 harness stamps
a constant — oracle §1c); recalc is only correct if the write path supplies true dates. Schema can enforce
`NOT NULL` but cannot enforce *truthful*; that is an SPI-contract note, not a CHECK.

## 4. D3 — per-consumed-layer linkage (from child (a))

A depletion can consume **multiple** layers (the layer-walk's `while remaining > 0`); the single
`trx_line.source_trx_line_id` cannot represent that. (a) requires per-layer granularity and left the *form* to
this child. Resolution:

> **D3 resolved: an explicit consumption-linkage table, not per-layer `posting_line` decomposition.**

```sql
cost_layer_consumption (
    depletion_trx_line_id  BIGINT NOT NULL REFERENCES trx_line(id),
    layer_trx_line_id      BIGINT NOT NULL REFERENCES trx_line(id),  -- the receipt whose layer was consumed
    recalc_generation      BIGINT NOT NULL,                          -- which pass produced this (see §5)
    qty                    BIGINT NOT NULL,                          -- qty taken from this layer
    unit_cost              BIGINT NOT NULL,                          -- the layer's cost
    PRIMARY KEY (depletion_trx_line_id, layer_trx_line_id, recalc_generation)
)
```

Why a table over decomposed `posting_line`s: (i) the depletion's authoritative unit_cost is *derivable* as
`banker_div(Σ qty·unit_cost, Σ qty)` over its generation-N rows — the linkage IS the cost basis, not a
side-record; (ii) it is the §14.4 Dynamics-style *marking* structure and the §13 "audit linkage between
depletions and the receipt layers that fed them" verbatim; (iii) generation-keying (§5) lets an R-2 re-cost
write a fresh generation without deleting the prior one, so the marking history is append-only and auditable.
Decomposing the GL leg into per-layer `posting_line`s instead would bloat the journal and still not give a
clean "which layers fed this depletion" query. The GL adjustment stays **one** `cost_adjustment_line` per
depletion (the net delta, §5); the per-layer detail lives here.

## 5. Idempotent re-run — the generation-delta model

This is the proof obligation (a)'s D2(i) full-replay makes tractable but does not by itself discharge:
full-replay makes the *computation* deterministic; the *GL posting* must also be idempotent so a repeated pass
(crash-retry, redundant trigger) posts nothing new while a genuine re-cost (backdated event) posts exactly the
delta.

**Per-pool settlement state:**

```sql
pool_settlement (
    pool_id                   BIGINT PRIMARY KEY REFERENCES pool(id),
    recalc_generation         BIGINT NOT NULL DEFAULT 0,   -- monotonic; bumped once per authoritative pass
    settled_through_posted_at TIMESTAMPTZ,                 -- business-time frontier costed authoritatively
    settled_through_id        BIGINT,                      -- within-date tiebreak of the frontier
    recost_floor_posted_at    TIMESTAMPTZ,                 -- earliest position needing re-cost (R-2); NULL = none
    recost_floor_id           BIGINT,
    last_recalc_at            TIMESTAMPTZ
)
```

- **`recost_floor_*`** implements R-2: when the feed delivers a receipt whose `posted_at` precedes
  `settled_through_posted_at`, set the floor to `min(current floor, that position)`. The next pass replays the
  pool from `≤ floor` (child (a)'s re-cost floor; child (b) may restore a checkpoint to it). A clean pass sets
  floor `= NULL` and advances `settled_through_*`.
- **`recalc_generation`** is the per-pool monotonic counter every authoritative write is stamped with.

**Per-depletion authoritative record (append-only):**

```sql
cost_settlement (
    depletion_trx_line_id      BIGINT NOT NULL REFERENCES trx_line(id),
    recalc_generation          BIGINT NOT NULL,
    authoritative_unit_cost    BIGINT NOT NULL,                       -- banker_div over this gen's consumption
    prior_unit_cost            BIGINT NOT NULL,                       -- provisional (gen 1) or auth of gen N-1
    adjustment_posting_line_id BIGINT REFERENCES posting_line(id),    -- the GL delta this gen posted (NULL if delta 0)
    PRIMARY KEY (depletion_trx_line_id, recalc_generation)
)
```

**The re-run contract (generation-delta):**

1. A recalc pass over pool P computes generation `N = recalc_generation + 1` deterministically from opening
   state + the `(posted_at, id)` stream (child (a), full-replay).
2. For each depletion it writes a `cost_settlement` row with `authoritative_unit_cost` (gen N) and
   `prior_unit_cost` = the gen-`N-1` authoritative (or the provisional `trx_line.unit_cost` for N = 1).
3. The **GL adjustment posted is the delta**: `(authoritative_N − prior) × qty`, one `cost_adjustment_line` +
   `posting_line` routed through `posting_account_map` (§3.7). **If the delta is 0, nothing is posted**
   (`adjustment_posting_line_id` NULL).
4. All gen-N writes (`cost_settlement`, `cost_layer_consumption`, `pool_state` layer rows, the adjustment
   trx/lines) are keyed by `recalc_generation = N` and done in **one transaction** that bumps
   `pool_settlement.recalc_generation` to N as its last act.

**Why this is idempotent.**
- *Repeated identical pass (crash-retry).* If a pass for P crashes before committing, `recalc_generation`
  never advanced; the retry recomputes the *same* N over the *same* stream → identical costs. The half-written
  gen-N rows (if any survived — they won't, it's one tx) are keyed by N and would `ON CONFLICT DO NOTHING`. Net
  new GL: zero. Full-replay determinism (a) is what guarantees "same N costs."
- *No-op re-cost.* If a pass runs but the stream is unchanged since gen N-1, every `authoritative_N ==
  authoritative_{N-1}` → every delta 0 → zero net GL. (A cadence that re-runs quiet pools, child (c), is
  therefore free of adjustment noise.)
- *Genuine backdated re-cost.* A backdated receipt sets `recost_floor`; the pass produces gen N with
  `authoritative_N ≠ authoritative_{N-1}` for the affected depletions → posts exactly the per-depletion delta.
  The append-only `cost_settlement` preserves both generations for audit; the "current" authoritative is the
  max-generation row.
- *Crash-recovery vs the stream.* The slot cursor (§17) advances only on the consumer's acknowledged flush, so
  a crash replays delivery from the last flushed LSN; the generation-keyed, single-tx pool writes make that
  replay safe. `pool_settlement` and the slot cursor are complementary, not redundant (§1).

This is **Model 1 (generation-delta, append-only)**. The considered alternative — **Model 2**, a `settled`
flag per depletion with in-place recompute-and-overwrite — is **rejected**: it loses the re-cost audit history,
needs bespoke crash handling (a half-overwritten depletion set has no clean generation boundary), and fights
the ledger's append-only ethos (`posting_line` append-only in the parent repo; `trx_line` append-only here).
Model 1's cost is storage (one `cost_settlement` row per depletion per generation, one
`cost_layer_consumption` row per consumed layer per generation); under a quiet steady state generations don't
grow (no-op passes post nothing and need not write a new generation — a pass with all-zero deltas can skip the
generation bump), so growth tracks genuine re-cost activity, which is the right thing to pay for.

## 6. `value_sum` reconciliation and variance-into-empty-pool

§7 floated a "possibly `total_value` on `pool_state`" column. **Not needed as a new column:** `pool_state`
already has `value_sum` (0007), the net posted book value, and its aggregate non-negative CHECK is already
dropped (0009). Recalc's role is to *reconcile* it: after an authoritative pass the aggregate `value_sum`
should equal Σ (layer `qty` × `unit_cost`) over the pool's open `layer_id > 0` rows, and the GL adjustment
legs are what move it from the provisional net to the authoritative net.

**Variance-into-empty-pool** (a depletion's authoritative cost when all layers are gone, or banker-rounding
residue with no surviving layer to absorb it) is real but is a **close-time / finalize concern → child (e)
acct-q1oj.5**, not a schema column here. (d) provides the hook: the residue is a `cost_adjustment_line`
against a variance account (via `posting_account_map`) whose `qty`-side is the empty pool, and `value_sum`
absorbs it as an aggregate-level figure (allowed negative, 0009). (e) decides *when* that residue is swept
(per-pass vs at close) and against which account.

## 7. DDL summary (sketch — final migrations are implementation follow-up)

| addition | shape | source |
|----------|-------|--------|
| enum labels | `trx_type += cost_adjustment`; `line_type += cost_adjustment_line`; `posting_event_type += cost_adjustment` | §2 / v3.1 §7 |
| sequence | `cost_adjustment_id_seq` for adjustment `trx.source_id` | §2 |
| `trx_line.posted_at` | denormalized `TIMESTAMPTZ` + index `(pool_id, posted_at, id)` | **D1** / child (a) |
| `cost_layer_consumption` | `(depletion_trx_line_id, layer_trx_line_id, recalc_generation, qty, unit_cost)` | **D3** / child (a), §13 audit |
| `pool_settlement` | per-pool generation + settled-through frontier + recost floor | §5, R-2 |
| `cost_settlement` | per-depletion-per-generation authoritative + prior + adjustment link (append-only) | §5 idempotency |

Migration granularity follows the project convention (one migration per concern; enums separate from tables).
None of these are on the PoC hot path except the `trx_line.posted_at` write; the recalc tables are written only
by recalc.

## 8. Interfaces to sibling children

- **← (a) acct-q1oj.1** — consumes (a)'s per-depletion output `(authoritative_unit_cost, [(layer, qty, cost)])`
  and re-cost-floor abstraction; resolves (a)'s D1 and D3.
- **→ (b) acct-q1oj.2** — the `pool_settlement.recost_floor_*` is what a checkpoint (D2(ii)/D4) restores the
  layer state *to*; the cross-pool scheduler reads/writes `pool_settlement` per pool. Checkpoint *storage* is
  (b)'s call; (d) only fixes the floor bookkeeping.
- **→ (c) acct-q1oj.3** — the generation-delta model makes no-op passes free (zero deltas → zero GL), which is
  what lets (c) run a tight cadence over quiet pools without adjustment noise; backlog depth reads from
  `settled_through_*` vs the slot frontier.
- **→ (e) acct-q1oj.5** — close reads `cost_settlement` (the authoritative valuation) and `pool_settlement`
  (is the pool fully settled, floor NULL?) to gate finalize; variance-into-empty-pool sweep (§6) is (e)'s.

## 9. Open design decisions (for review)

- **D5** — retain or drop the 0010 `(pool_id, id)` index once `(pool_id, posted_at, id)` exists (§3): drop iff
  no non-recalc consumer needs pure id-order. Low-stakes; decide when the migration lands.
- **D6** — whether a no-op (all-zero-delta) pass bumps `recalc_generation` at all (§5 recommends **not**, to
  keep generation growth proportional to genuine re-cost). Interacts with child (c)'s cadence accounting.
- **D7** — `posting_event_type` distinct `cost_adjustment` vs reuse `variance` (§2): recommended distinct;
  confirm against the eventual reporting requirements (close-time, child (e)).

D5–D7 are storage/labeling refinements; none change the idempotency contract (§5), which is the load-bearing
output of this child.
