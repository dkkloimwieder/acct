# Phase 1 planning inventory — Part IV §3 vs Phase 0 shipped

This is a **read-once working document** produced at the Phase 0 → Phase 1 boundary (2026-04-30). It maps every transaction pattern in the consolidated doc Part IV §3 against what the Phase 0 ledger actually shipped, surfaces the schema and invariant gaps that Phase 1 must close, and presents 3-4 candidate feature slices the user can pick from.

After a slice is picked, it becomes a Phase 1 epic in beads and this document sits as historical record. It is **not** maintained alongside ongoing Phase 1 work.

## 1. Context & gating

### Phase 0 status (functionally complete)

- 21 sequential reversible migrations (`0001_enable_extensions` through `0021_post_transfers_wac`).
- `post_transfers` with the `_post_transfers_compute_amount` cost dispatcher: real `'standard'` and `'wac'` branches; `'fifo'`/`'lot'` raise `P0006`.
- `reserve_inventory()` (PL/pgSQL function with FOR UPDATE + post-lock SELECT, fixes the snapshot-semantics bug B1).
- `run_daily_reconciliation()` + per-ledger double-entry invariant (B3 fix).
- 26 integration test binaries: per-invariant probes, conformance harness (107 cases / 11 batch-vs-split), WAC integration suite, and the 13-shape perf load matrix.
- `perf_baseline_v0.md` — baseline established; `acct-c4p` filed for the eventual sync→pseudo-sync (shape L) pivot when Phase 1 contention emerges.

### What unblocked Phase 1 entry

| Gate | Resolved | Where |
|---|---|---|
| Q1 — TB optionality | Yes (2026-04-29) | Doc Part VII Q1; "TigerBeetle is a reference model, not a parity target" |
| Q2 — TPS projection | Yes (2026-04-29) | Doc Part VII Q2; "no fixed TPS target; correctness > performance" |
| Q3 — outbox load-bearing | Yes (2026-04-30, re-resolved) | Doc Part VII Q3; sync now, pivot to L tracked as `acct-c4p` |
| Q4 — cost method | Yes (2026-04-30) | Doc Part VII Q4; standard + WAC shipped, fifo/lot under `acct-8gg` |
| §14.1 perf baseline | Yes | `perf_baseline_v0.md` |
| Phase 0 invariant tests | Yes | T2 + T3 + T5 + perf load suite |

### What's still open (scope-shaping for Phase 1)

| # | Question | Default | Forced by which slice |
|---|---|---|---|
| Q5 | Reservation lifetime — sub-second timeouts? | `pg_cron` 30s; `LISTEN/NOTIFY` only if needed | Slice C |
| Q6 | Append-only enforcement — trigger / RBAC / both? | Trigger present; RBAC layer not added | Any slice writing transfers |
| Q7 | CDC sinks at MVP | None | Not forced by Phase 1; deferred |
| Q8 | Commodity materiality threshold | 5% placeholder | Slice A only if commodity flow is in scope |
| Q9 | Tier-2 mat view scope | None; promote on signal | Slice B (per-op WIP queries) |
| Q10 | Per-WO per-op account opt-in | None | Slice B |

These do not block Phase 1 entry — they get answered as the chosen slice forces them.

## 2. Part IV §3 status table

Every named `transfer_reason` in the enum (`db/migrations/0002_enum_types.up.sql`), grouped by §3 sub-section. Conformance counts come from `tests/data/conformance.json`. "Workflow" is uniformly **NO** in Phase 0 — it is included to make the gap explicit.

### §3.1 PO receipt (firm-priced)

| reason | ledger primitive | conformance | dedicated test | document workflow |
|---|---|---|---|---|
| `po_receipt` | yes | 4 cases | none (only conformance) | no |
| `po_receipt_provisional` | yes (via 0011) | 1 case | none | no |
| `po_return_to_vendor` | yes | 1 case | none | no |
| `ppv` | yes | 1 case | none | no |

### §3.2 Inter-location transfer

| reason | ledger primitive | conformance | dedicated test | document workflow |
|---|---|---|---|---|
| `bin_move` | yes | 1 case | none | no |
| `to_release` | yes | 2 cases | none | no |
| `to_receipt` | yes | 1 case | none | no |

### §3.3 SO reservation, allocation, ship

| reason | ledger primitive | conformance | dedicated test | document workflow |
|---|---|---|---|---|
| `so_ship` | yes | 10 cases | none | no |
| `customer_return` | yes | 2 cases | none | no |
| (reservation primitive — `reserve_inventory()`) | yes | n/a | T3 (4 dedicated tests) | partial — primitive shipped; no SO state machine |

### §3.4 Work order lifecycle

| reason | ledger primitive | conformance | dedicated test | document workflow |
|---|---|---|---|---|
| `rm_issue_to_wo` | yes | 3 cases | none | no |
| `wo_start` | yes | 2 cases | none | no |
| `op_move` | yes | 6 cases | none | no |
| `wo_complete` | yes | 4 cases | none | no |
| `wo_close_v` | yes | 1 case | none | no |
| `rework` | yes | 1 case | none | no |
| `labor_apply` | yes | 1 case | none | no |
| `oh_apply` | yes | 1 case | none | no |
| `muv` | yes | 1 case | none | no |
| `lv` | yes (enum) | **0 cases** | none | no |
| `ohv` | yes (enum) | **0 cases** | none | no |

### §3.5 Scrap at operation

| reason | ledger primitive | conformance | dedicated test | document workflow |
|---|---|---|---|---|
| `scrap` | yes | 2 cases | none | no |
| `scrap_v` | yes (enum) | **0 cases** | none | no |
| `damage` | yes | 1 case | none | no |

### §3.6 Quarantine and release

| reason | ledger primitive | conformance | dedicated test | document workflow |
|---|---|---|---|---|
| `quarantine` | yes | 1 case | none | no |
| `release_from_quarantine` | yes | 1 case | none | no |

`qc_holds` table is referenced in §3.6 but **does not exist in any migration**.

### §3.7 AR / AP payment (and the undocumented invoice/bill side)

| reason | ledger primitive | conformance | dedicated test | document workflow |
|---|---|---|---|---|
| `ar_payment` | yes | 3 cases | none | no |
| `ap_payment` | yes | 1 case | none | no |
| `ar_invoice` | yes (enum) | **79 cases** (heaviest reason) | none | **no — and not narrated in §3.7** |
| `ap_bill` | yes (enum) | 9 cases | none | **no — and not narrated in §3.7** |

§3.7 narrates only payment. `ar_invoice` and `ap_bill` are first-class enum values, are exercised heavily in conformance, but have no §3 narrative. **This is the largest doc-vs-code drift in §3** and any inbound or outbound slice will force its resolution.

### §3.8 Cycle-count adjustment

| reason | ledger primitive | conformance | dedicated test | document workflow |
|---|---|---|---|---|
| `cycle_count_adj` | yes | 32 cases | none | no |

Heavily used in conformance preconditions (32 cases) but no document-layer cycle-count workflow.

### §3.9 Reversals

| reason | ledger primitive | conformance | dedicated test | document workflow |
|---|---|---|---|---|
| `reversal` | yes | 2 cases | none | no |
| `cost_restate` | yes | 2 cases | none | **no — and not narrated in §3** |

`cost_restate` is enumerated and tested but absent from the §3 narrative.

### §3.10 Cross-currency transfer

| reason | ledger primitive | conformance | dedicated test | document workflow |
|---|---|---|---|---|
| `fx_leg` | yes | 1 case | none | no |
| `fx_spread` | yes | 1 case | none | no |

### Commodity provisional pricing settlement (§10, referenced from §3.9 reversals)

| reason | ledger primitive | conformance | dedicated test | document workflow |
|---|---|---|---|---|
| `po_settlement` | yes | 1 case | none | no |
| `price_settlement` | yes (enum) | **0 cases** | none | no |
| `price_trueup_inventory` | yes (enum) | 1 case | none | no |
| `price_trueup_cogs` | yes (enum) | **0 cases** | none | no |
| `price_trueup_wip` | yes (enum) | **0 cases** | none | no |

### Coverage summary

- **39 transfer reasons** in the enum.
- **33** have ≥1 conformance case (≈85% coverage).
- **6** are enumerated but never exercised: `lv`, `ohv`, `scrap_v`, `price_settlement`, `price_trueup_cogs`, `price_trueup_wip`.
- **Zero** have a dedicated document-layer workflow. All are exercised as ledger primitives only.
- **Heaviest reasons by conformance volume**: `ar_invoice` (79), `cycle_count_adj` (32), `so_ship` (10), `ap_bill` (9), `op_move` (6).

## 3. Schema gaps

Tables Part IV §3 (or the surrounding sections) reference, that **are not in any migration**:

| Table | Referenced in | Required for |
|---|---|---|
| `customers` | §3.3 (`accounts(ar, customer_id, ...)`); reservation rows reference `so_id` which implicitly links to a customer | AR posting, customer master data, FK target for `sales_orders.customer_id` |
| `suppliers` | §3.1 (`accounts(supplier_pool, supplier_id, ...)`, `accounts(ap, supplier_id, ...)`) | AP posting, supplier master data, FK target for `purchase_orders.supplier_id` |
| `purchase_order_lines` | §3.1 (PO receipt closes/reduces "the line's open qty") | Per-line open-qty conservation; AP-bill matching against PO lines |
| `sales_order_lines` | §3.3 (reservation references `so_line_id`) | Per-line reservation/allocation/ship state; AR-invoice matching against SO lines |
| `work_orders` | §3.4 ("WO start", "WO complete", state machine) | WO state machine, document_id FK target |
| `routings` | §3.4 (Op 10, Op 20 sequence) | Operation-precedence enforcement, per-op std cost lookup |
| `routing_operations` | §3.4 (`routing_op` integers as op identifiers) | Per-op std cost (currently bare integers on transfers) |
| `bom_items` (or equivalent) | §3.4 (`rm_issue_to_wo` for component issue) | Component-qty validation against parent BOM |
| `qc_holds` | §3.6 (referenced explicitly) | Quarantine state, authorizer, test results, release date |

What **does** exist as schema scaffolding:
- `skus` (real reference table — code, uom, standard_cost, cost_method).
- `locations` (real reference table — code, name).
- `sales_orders` and `purchase_orders` (bare stubs — id, customer_id/supplier_id UUID columns with no FK target, `status TEXT`, no lines).
- `commodity_receipts` (migration 0011 — provisional-pricing tracking).

## 4. Invariant gaps

State-machine rules §3 documents but Phase 0 doesn't enforce. Phase 0 is primitive-only by design — these belong to Phase 1 document workflows.

### From §3.1 — PO receipt
- PO must be in `'open'` or `'released'` state before a receipt can post.
- Receipt closes or reduces `purchase_order_lines.open_qty`. Cumulative receipts cannot exceed the line's ordered qty.
- `po_receipt_provisional` (commodity, §10) implies a settlement window in which the line is unsettled.

### From §3.3 — SO reservation/allocation/ship
- SO must be in `'open'` state before a reservation can be created.
- Reservation must be `'allocated'` before a `so_ship` can post.
- Reservation + ship are **atomic in the caller's transaction** — the ship event also marks the reservation as allocated/shipped.
- `inventory_reservations` already enforces the qty-promisable invariant (B1 fix); the SO-state-machine layer is what's missing.

### From §3.4 — WO lifecycle
- WO must transition: `created → released → in-progress → completed → closed`.
- `op_move` requires WO state ∈ `{released, in-progress}`.
- `op_move` to Op 20 requires non-zero qty in `stock_wip` at Op 10 (op-precedence).
- `rm_issue_to_wo` qty must not exceed BOM qty for that (parent_sku, comp_sku, parent_qty).
- `wo_complete` requires WO at the last routing op.
- `wo_close_v` posts only after `wo_complete` and is the WO's final transfer.

### From §3.5 — Scrap variance
- `scrap_v` amount must equal accumulated cost of scrapped qty (computed read-then-write under lock — same pattern as `op_move`).

### From §3.6 — QC hold release
- `release_from_quarantine` requires a `qc_holds` row with authorizer + test results + release date.

### From §3.7 — AR/AP matching (implied, not narrated)
- `ar_invoice` posts against an open SO line OR a standalone service line (no inventory side). Total invoiced ≤ total shippable.
- `ap_bill` posts against an open PO line OR a standalone expense line (no inventory side). Three-way match: PO line × receipt × bill.
- `ar_payment` reduces `ar` for a specific customer; aging would track open-invoice age.
- `ap_payment` reduces `ap` for a specific supplier; aging would track open-bill age.

### From §3.9 — Reversal authorization
- A reversal cannot be posted against a closed period without `override=TRUE`.
- Document-layer rule: who can authorize a reversal? Phase 0 has no RBAC.

### From §6 — Period close
- Period close requires `run_daily_reconciliation()` to return zero alerts.
- Period-close transition is itself a document action with an authorizer.

## 5. Inconsistencies surfaced

| # | Inconsistency | Type | Recommended fix |
|---|---|---|---|
| 5a | `ar_invoice`, `ap_bill` heavily tested (79 + 9 cases) but absent from §3 narrative | doc gap | fix doc — write §3.7 invoice/bill section, OR fold into chosen Phase 1 slice's design |
| 5b | `lv`, `ohv` enumerated and documented as variances in §3.4 but never exercised in conformance | test gap | add conformance cases as part of WO slice (B), OR remove from enum if intentionally deferred |
| 5c | `scrap_v` enumerated and documented in §3.5 but never exercised | test gap | add conformance case as part of WO slice (B) |
| 5d | `qc_holds` table referenced in §3.6 but does not exist in migrations | schema gap | add as part of any slice that includes quarantine; not on the critical path of A/B/C |
| 5e | `cost_restate` shipped + 2 conformance cases but no §3 narrative | doc gap | add brief §3.11 narrative, OR fold into Phase 1 slice that exercises it |
| 5f | `price_settlement`, `price_trueup_cogs`, `price_trueup_wip` enumerated for commodity settlement but never exercised | test gap | commodity workflow is its own future slice (not in A/B/C/D); leave for now |
| 5g | `sales_orders` / `purchase_orders` are bare stubs with `customer_id` / `supplier_id` UUID columns but no `customers` / `suppliers` master | schema gap | resolved by Slice A (suppliers) and Slice C (customers) |
| 5h | `routing_op` is a bare INTEGER on transfers; no `routings` or `routing_operations` table to validate it | schema gap | resolved by Slice B |

Items 5a, 5b, 5c, 5e are the doc-vs-code drift the inventory was specifically asked to surface. Each is small enough to fold into the chosen slice's design rather than file as a standalone issue.

## 6. Candidate Phase 1 feature slices

Slices are framed by **transactional-cycle position**: inflow → conversion → outflow → cross-cutting. Inventory has to come in (or be adjusted in) before it can be transformed or shipped, so receiving / AP / inventory-adjustment is the natural foundational entry point.

For each slice: required schema additions, required invariants, expected test additions, P3 dependencies, and which §3 sub-sections it activates.

---

### Slice A — Inbound: PO receipt + vendor bill + AP payment + inventory adjustment

**Cycle position:** Inflow (foundation). Brings raw material / FG into the system and pays for it; also covers the standalone "we counted, we adjust" path that doesn't need a PO.

**Already-shipped primitives this slice activates:** `po_receipt`, `po_receipt_provisional`, `po_return_to_vendor`, `ppv`, `ap_bill`, `ap_payment`, `cycle_count_adj`. (§3.1 + §3.7 AP-side + §3.8.)

**Required schema:**
- `suppliers` (master: id UUID, code, name, currency default, terms, FK target for AP).
- `purchase_orders` — promote from stub: add line linkage, totals, currency.
- `purchase_order_lines` (PO line: po_id, line_no, sku_id, qty_ordered, unit_cost, currency, status, FK to purchase_orders).
- Optional: `ap_bills` and `ap_bill_lines` for explicit invoice tracking + three-way match against PO line × receipt × bill. Phase 1 minimum: implicit linkage via `transfers.document_id`.

**Required invariants:**
- PO state machine: `open → released → received → closed`.
- Per-line: cumulative `po_receipt` qty ≤ `qty_ordered`; over-receipt requires explicit override or new PO line.
- `ap_bill` against an **open PO line** OR an **expense account** (the "paying for services" path — utilities, consulting, where there is no inventory side). The two paths must be cleanly distinguishable.
- `ap_payment` reduces `ap(supplier_id, currency)` balance; cumulative payments ≤ open AP balance for that supplier.
- `cycle_count_adj` posts both qty-side and value-side; standalone (does not require a PO).

**Expected test additions:**
- Document workflow tests for PO open → release → receipt → close (per-line invariants).
- Doc-vs-code drift resolution: write §3.7 invoice/bill narrative as part of slice design.
- Conformance additions if any new reasons emerge from the matching logic (likely none — primitives already cover it).
- `lv` / `ohv` / `scrap_v` are NOT in this slice's scope (they belong to B).

**P3 dependencies:**
- `acct-c4p` (sync→pseudo-sync) — not blocking. Slice A's calls are document-driven (one PO receipt at a time), well under L's contention regime.
- `acct-e8g` (transfers partitioning) — not blocking. Volume implications only.
- `acct-8gg` (additional cost methods) — not blocking. WAC already covers the test SKU; FIFO/lot deferred.

**Doc-vs-code drift this slice resolves:** 5a (partial — AP-side narrative), 5g (suppliers schema gap).

**Forces which open Q5-Q10:** Q6 partially (RBAC for AP-payment authorization is implicitly raised but can be deferred).

**Effort estimate:** smallest schema add of A/B/C; cleanest scope. Can ship in a small number of migrations + a focused integration-test suite.

---

### Slice B — Conversion: WO lifecycle + per-op WIP + variances

**Cycle position:** Conversion (middle of cycle). Depends on having raw material in stock — sequenced after Slice A in the natural flow but technically independent if the test fixture seeds raw material directly.

**Already-shipped primitives this slice activates:** `rm_issue_to_wo`, `wo_start`, `op_move`, `wo_complete`, `wo_close_v`, `rework`, `labor_apply`, `oh_apply`, `muv`, plus the dormant `lv`, `ohv`, `scrap_v` (currently 0 conformance cases).

**Required schema:**
- `work_orders` (state machine: `created → released → in-progress → completed → closed`; FK to skus for parent product, expected qty, dates).
- `routings` (per-SKU operation sequence definition).
- `routing_operations` (per-op std labor + std overhead + std setup; FK to routings; sequence order).
- `bom_items` (or `routing_components` — per-op component requirement; FK to routings or to skus directly, with comp_sku + qty_per).

**Required invariants:**
- WO state machine enforcement on every WO-related transfer.
- Op-precedence: `op_move` to Op N requires non-zero qty in `stock_wip` at Op N-1 for the same WO.
- BOM validation: `rm_issue_to_wo` total qty for (parent_sku, comp_sku) ≤ BOM `qty_per` × WO qty.
- `scrap_v` and `wo_close_v` use read-then-write under FOR UPDATE (already-supported pattern from acct-uxu / WAC).
- `lv` (labor variance) and `ohv` (overhead variance) post at WO close based on actual vs std accumulated.
- Per-op WIP queries (e.g., "value in WIP right now by op") are tier-1 base-table queries — Q9 (mat-view scope) is forced by the perf characteristics of these queries on a non-toy fixture.

**Expected test additions:**
- Conformance additions for `lv`, `ohv`, `scrap_v` (currently 0 cases each).
- Document workflow tests for WO open → release → start → op_moves → complete → close.
- Variance correctness tests under the `'standard'` cost method.

**P3 dependencies:**
- `acct-8gg` (additional cost methods) — could become relevant if WAC under WO concurrency surfaces issues. Decision deferred.
- `acct-e8g` (transfers partitioning) — could be relevant if WO traffic creates volume hot spots; revisit on perf re-baseline post-slice.

**Doc-vs-code drift this slice resolves:** 5b, 5c, 5h.

**Forces which open Q5-Q10:** Q9 (tier-2 mat view scope — per-op WIP queries), Q10 (per-WO per-op account opt-in).

**Effort estimate:** **largest schema add** of A/B/C/D. Routings + BOMs + WOs is meaningful schema design work. Highest learning value too — this slice exercises the most of §3 and the most of the cost dispatcher.

---

### Slice C — Outbound: SO + reservation → ship + AR invoice + AR payment

**Cycle position:** Outflow (end of cycle). Depends on inventory existing (Slice A or test-seeded), and ideally on FG existing (Slice B for in-house production; not required if SO ships purchased FG).

**Already-shipped primitives this slice activates:** `so_ship`, `ar_invoice`, `ar_payment`, `customer_return`, plus the existing `inventory_reservations` table + `reserve_inventory()` function.

**Required schema:**
- `customers` (master: id UUID, code, name, currency default, terms, FK target for AR).
- `sales_orders` — promote from stub: add line linkage, totals, currency.
- `sales_order_lines` (so_id, line_no, sku_id, qty_ordered, unit_price, currency, status, FK to sales_orders).
- Optional: `ar_invoices` / `ar_invoice_lines` for explicit invoice tracking + matching against SO lines + ship events.

**Required invariants:**
- SO state machine: `open → reserved → allocated → shipped → invoiced → paid → closed`.
- Per-line reservation: `inventory_reservations.so_line_id` must FK to a real line; one active reservation per line.
- `so_ship` requires reservation in `'allocated'` state for the same `so_line_id`; ship marks reservation `'shipped'` (or `'allocated'` plus a separate "ship status").
- `ar_invoice` against an open SO line (with shipped qty) OR a standalone service line.
- `ar_payment` reduces `ar(customer_id, currency)` balance; cumulative payments ≤ open AR balance for that customer.
- Reservation expiry (`pg_cron`, 30s) already handles abandonment — Q5 forced if sub-second timeouts ever needed.

**Expected test additions:**
- Document workflow tests for SO open → reserve → allocate → ship → invoice → pay → close.
- Doc-vs-code drift resolution: write §3.7 AR invoice narrative as part of slice design.
- Customer return path tests (`customer_return` reason — currently 2 conformance cases, no document workflow).

**P3 dependencies:**
- `acct-c4p` — not blocking, but SO-ship is a higher-volume path than PO receipt and could expose contention sooner. If a hot AR account emerges (e.g., a single high-traffic customer), this is the slice that would trigger the L pivot.
- `acct-e8g` — not blocking.
- `acct-8gg` — not blocking.

**Doc-vs-code drift this slice resolves:** 5a (partial — AR-side narrative), 5g (customers schema gap).

**Forces which open Q5-Q10:** Q5 (reservation lifetime — sub-second timeouts may be needed if a high-traffic ecommerce-style flow is in scope).

**Effort estimate:** comparable to Slice A in schema; somewhat larger in invariants because the SO state machine is longer than the PO state machine and the reservation interaction adds complexity.

---

### Slice D — Cross-cutting: Period close + reconciliation hardening

**Cycle position:** Cross-cutting. No new tables. Tightens existing primitives; can ship in parallel with any of A/B/C or as a discrete first-step warm-up.

**Required schema:** none (uses existing `periods`, `period_snapshots`, `run_daily_reconciliation`).

**Required invariants:**
- Period close requires `run_daily_reconciliation()` to return zero alerts.
- Period close transition is a document action with an authorizer (RBAC layer — Q6 forced if scoped here).
- `period_snapshots` row created on period close, capturing balance state for audit.
- Override (`override_closed_period=TRUE`) for posts to closed periods is logged with the override-er identity.

**Expected test additions:**
- Period-close success path on a richer fixture (not the conformance-style fixture).
- Period-close failure path: reconciliation returns alerts → close blocks.
- `period_snapshots` content verified against pre-close balances.
- Override audit-log test.

**P3 dependencies:** none.

**Doc-vs-code drift this slice resolves:** none (Slice D is purely about exercising existing primitives more rigorously).

**Forces which open Q5-Q10:** Q6 (RBAC) only if authorizer audit is in scope.

**Effort estimate:** smallest of all four. Can be a parallel track or a 1-2 week warm-up before committing to a heavier slice.

---

### Natural sequencing

```
A (inflow) ──► B (conversion, depends on having raw in stock)
            └► C (outflow, depends on having FG in stock)
D (cross-cutting, ships any time)
```

The user can pick any slice. A and D are the lowest-risk first picks. B has the largest schema add. C has the most state-machine complexity.

## 7. Open Q5-Q10 forcing functions

| Open Q | Drives | Forced by |
|---|---|---|
| Q5 — Reservation lifetime | `pg_cron` (30s) vs `LISTEN/NOTIFY` (sub-second) | Slice C if a flow demands sub-second reservation expiry |
| Q6 — Append-only enforcement model | Trigger present; RBAC layer not added | Any slice that introduces document-layer authorization (likely C and D first) |
| Q7 — CDC sinks at MVP | None / search / OLAP | Not forced by any A/B/C/D slice; deferred |
| Q8 — Commodity materiality threshold | 5% placeholder | Not forced — commodity workflow is a separate future slice |
| Q9 — Tier-2 mat view scope | None; promote on signal | Slice B (per-op WIP queries become hot under realistic concurrency) |
| Q10 — Per-WO per-op account opt-in | None | Slice B (the per-op accounts are the natural opt-in point) |

## 8. Transactional-cycle framing

```
                ┌──────────────────────────────────────────────────┐
                │                  CROSS-CUTTING                   │
                │  Slice D:  period close + reconciliation         │
                │            (no new tables, RBAC opt-in)          │
                └──────────────────────────────────────────────────┘

  ┌───────────────┐       ┌───────────────────┐       ┌───────────────┐
  │   INFLOW      │       │   CONVERSION      │       │   OUTFLOW     │
  │   Slice A     │ ────► │   Slice B         │ ────► │   Slice C     │
  │               │       │                   │       │               │
  │ • PO receipt  │       │ • WO lifecycle    │       │ • SO + reserve│
  │ • AP bill     │       │ • Per-op WIP      │       │ • Ship + AR   │
  │ • AP payment  │       │ • Variances       │       │ • AR invoice  │
  │ • Cycle count │       │   (lv,ohv,scrap_v)│       │ • AR payment  │
  │   adjust      │       │                   │       │ • Cust return │
  │               │       │                   │       │               │
  │ Schema:       │       │ Schema:           │       │ Schema:       │
  │ +suppliers    │       │ +work_orders      │       │ +customers    │
  │ +po_lines     │       │ +routings         │       │ +so_lines     │
  │ (+ap_bills?)  │       │ +routing_ops      │       │ (+ar_invoices?│
  │               │       │ +bom_items        │       │               │
  │               │       │                   │       │               │
  │ §3.1 §3.7-AP  │       │ §3.4 §3.5         │       │ §3.3 §3.7-AR  │
  │ §3.8          │       │                   │       │               │
  └───────────────┘       └───────────────────┘       └───────────────┘
```

Already-shipped primitives at each stage (no new ledger work):
- **Inflow**: `po_receipt`, `po_receipt_provisional`, `po_return_to_vendor`, `ppv`, `ap_bill`, `ap_payment`, `cycle_count_adj`.
- **Conversion**: `rm_issue_to_wo`, `wo_start`, `op_move`, `wo_complete`, `wo_close_v`, `rework`, `labor_apply`, `oh_apply`, `muv`, `scrap`, `damage`, `lv`*, `ohv`*, `scrap_v`* (* = enumerated, never exercised).
- **Outflow**: `so_ship`, `ar_invoice`, `ar_payment`, `customer_return`.
- **Cross-cutting**: `reversal`, `cost_restate`, `fx_leg`, `fx_spread`, `bin_move`, `to_release`, `to_receipt`, `quarantine`, `release_from_quarantine`.

## 9. What this doc is NOT

- **Not** a Phase 1 design spec. The actual design lives in a Phase 1 epic in beads after the slice is picked.
- **Not** maintained alongside Phase 1 work. After the slice is picked and the epic is filed, this doc sits as historical record.
- **Not** prescriptive about *which* slice to pick. The candidates are presented in cycle order with their natural dependencies; the user chooses.
- **Not** a re-litigation of Part VII Q1-Q4 (resolved) or D3 (re-resolved as `acct-0oy` / `acct-c4p`).
- **Not** a vehicle for fixing doc-vs-code drift inline. Drift is surfaced (§5); resolution happens as part of the chosen slice's design or as a separate doc-catchup if no slice resolves it.
