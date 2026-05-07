# Architecture Synthesis: research/ vs the shipped codebase

**Status:** v0 working draft, 2026-05-06.
**Scope:** Compares the three documents under `research/` against the architecture shipped across migrations 0001–0101 (647 tests, ci-check clean at d9cd9ee). Recommends, does not decide.
**Audience:** project owner; future-self; prospective contributors trying to understand where the project sits relative to industry-leading ERPs and the alternate Postgres-ledger proposal.
**Out of scope:** edits to `ledger_design_consolidated_v0.md` (see §11), edits to migrations or code, actual `bd create` invocations (the §10 catalog is the prep).

---

## §1 Executive Summary

We compared the shipped architecture against three research documents (4,749 lines combined): an ERP industry survey of SAP S/4HANA / Oracle Fusion Cloud / Microsoft D365 F&O / Oracle NetSuite [`erp-transaction-architectures-revised.md`]; a critical treatise on cost methods and the **GL-aggregates / subledger-details** principle [`cost-methods-subledger-design.md`]; and an alternate Postgres-native ledger architecture proposal [`ledger-architecture-proposal.md`]. The three are internally coherent and converge on a small set of architectural commitments that re-cast our work in a useful light.

**Headline finding.** Across 17 architectural dimensions surveyed (§5):
- **6 are confirmed-correct** — append-only enforcement, idempotency, period-close orchestration, multi-currency at row grain, registry-driven cost-method dispatch, bi-temporal `business_date` + `posted_at` semantics.
- **5 are aligned-but-shallow** — the shape is right, the implementation is functional but narrower than research prescribes (multi-currency, period close, reconciliation, drill-back, dispatch).
- **4 are deferred-gaps** — already filed as `bd` issues (FIFO/lot/serial subledger tables, account-kind enum→table, partitioning, multi-entity).
- **2 are drift-action** — the consequential ones, surfaced by this synthesis: **(a) `transfers` conflates the GL-posting-line role with the inventory-event-grain role**, which works at our scale but breaks the moment serialized inventory or lot-based costing lands; **(b) the "ledger" dimension is implicit in our schema** (single ledger, no parallel-accounting / posting-layer column), which has zero present cost but eliminates a future capability with non-trivial unwind.
- **0 are overreach** — nothing the research advocates is silly. (We will not adopt the Tier-2 WASM rules engine, but that's a deliberate scoping cut, not a disagreement — see §8.)

**The central insight that re-frames everything else:** the alternate proposal is **GL-first with inventory as a thin extension**; our codebase is **inventory-first with the GL emerging from `transfers`**. Both paths can be made correct. The research argues the GL-first path scales better at high inventory granularity (lots, serials) and supports parallel accounting more naturally; the inventory-first path is faster to ship and easier to reason about at single-ledger / aggregate-cost grain. We're on the inventory-first path by accumulated decision, not by repudiation of the alternative. Acknowledging this explicitly — rather than slowly rediscovering it as we add capability — is the highest-leverage outcome of this synthesis.

**Top 5 actionable items** (full catalog in §10):

1. **`acct-8gg` (FIFO/lot/serial cost methods) is now blocked-by an architectural decision:** do we add a method-specific subledger table when those methods land (research-prescribed), or extend `transfers.qty` semantics to carry layer / lot / unit references inline? Decision belongs in `ledger_design_consolidated_v0.md`. P2.
2. **Add a cheap, additive `posting_layer` column to `transfers`** (defaults to `1` = "current"; current behavior preserved). Cost: single nullable BIGINT + index. Buys: future-proof for IFRS-vs-local-GAAP / tax-basis / management-adjustment tagging without a `transfers` rewrite. Phase 2 / P3.
3. **Document `transfers` semantics explicitly** — what it is (the universal posting-line store) and what it isn't (a per-document subledger of inventory movements). The current shape works; future contributors should not assume it generalizes to `inventory_movements` SAP/Oracle-style. CLAUDE.md update + `ledger_design_consolidated_v0.md` Part IV cross-reference. P3.
4. **Lift `acct-2thf`** (account_kind enum → row table) **from "threshold-gated"** (we said ~80 values; we're at 46) **to "do it anyway"**. The enum-driven model is a known-bad idea; the fact that we're not yet hurting doesn't change the conclusion. Cost is moderate, the unblock-effect on multi-entity / chart-of-accounts work (acct-3gzh, acct-w1v3) is real. P2.
5. **Add a `legal_entity_id` column to `transfers` and `accounts`** as a no-op SMALLINT NOT NULL DEFAULT 1, indexed. Single-entity behavior preserved; future multi-entity work (acct-3gzh) gets a free zero-migration foundation. Phase 2 / P3.

If you stop reading here, those five are the actionable beachhead. The rest of the document explains why each is right, where we sit on each axis, and what it costs to converge.

---

## §2 Provenance

The three input documents under `research/`:

### §2.1 `erp-transaction-architectures-revised.md` (1,039 lines)
A descriptive survey of how the four leading commercial ERPs structure their transaction architectures. Chapters per vendor (SAP S/4HANA §3; Oracle Fusion §4; D365 F&O §5; NetSuite §6) catalog event taxonomies, posting-derivation mechanisms, parallel-accounting models, and modular integrations. §7 ("Toward a Unified Model") synthesizes a 12-category × ~89-event reference taxonomy, a 15-item modern-ERP capability set, and an "optimal" reference architecture. §8 documents corrections from a prior version (which makes the doc credible — it is self-correcting).

**Authoritative sources cited:** SAP Help Portal, Oracle Cloud Documentation 25B–26B, Microsoft Learn, Oracle NetSuite Application Help. **Not relied on:** analyst material, partner blogs.

**Strengths:** the §7 unified taxonomy is concrete enough to cross-walk against our `transfer_reason` enum and `post_*` functions (Appendix A does this). The §7.2 capability list lets us answer "what % of a modern ERP have we shipped?" in a defensible way.

**Caveats:** the doc is descriptive, not prescriptive about implementation. It says "what" but largely punts on "how", deferring the how to the alternate proposal.

### §2.2 `cost-methods-subledger-design.md` (2,126 lines)
A treatise centered on a single architectural commitment: **the GL records aggregate financial impact; the subledger records per-unit / per-lot / per-layer detail**. §2 develops the principle with verbatim definitions of what GL records (§2.1), what subledger records (§2.2), the reciprocal linkage (§2.3), why the separation is non-optional at scale (§2.4), and how append-only inherits to subledgers (§2.5). §3 enumerates a three-axis classification matrix (real-vs-reference cost; aggregated-vs-layered-vs-identified; perpetual-vs-periodic). §§4–11 walk seven cost methods (standard, moving-average perpetual, weighted-average periodic, FIFO, LIFO, lot-based, serialized) with concrete subledger schemas and operational mechanisms. §16 lays out a six-phase implementation order. §17 closes with the dominant pattern: GL aggregates + subledger details + reciprocal linkage + both-append-only + reconciliation invariants + materialized current-state.

**Strengths:** the schema-per-cost-method specifics are concrete enough to assess our deferred FIFO/lot/serial work (`acct-8gg`, `acct-uze`, `acct-0kz`) against. The volume table in §15 makes the scaling argument empirically: serialized scenarios are 25–50× the GL volume, lot scenarios 10–20×, FIFO 5×.

**Caveats:** the doc takes "the `posting_lines` schema" — i.e., the structure from the third document — "as given". So §2.4's "subledger separation is non-optional" is conditional on first having a GL/subledger split in the schema. Our `transfers` doesn't separate them; the doc's conclusion lands somewhere different in our context (analyzed in §5.2).

### §2.3 `ledger-architecture-proposal.md` (1,584 lines)
A prescriptive alternate Postgres-native ledger architecture. §1 establishes three principles: data-model and rule-layer separability; platform-aware design (Postgres row-store ≠ HANA column-store); configuration-first (code as escape hatch). §2 specifies a `posting_lines` core (~17 columns) + `postings` header + five typed extension tables (dimensions, multi-currency, source-linkage, inventory, custom-segments). §3 specifies a two-tier rules engine: Tier 1 configuration (rule_sets / journal_line_rules / account_rules / mapping_sets), Tier 2 WASM modules. §4 covers cross-cutting concerns: outbox, idempotency, bi-temporal modeling, derived-state projections, multi-currency, period close, reconciliation, cross-shard invariants. §5 synthesizes what the design achieves and explicitly what it cannot.

**Strengths:** the schema is concrete enough to compare column-by-column against ours. The §5.3 "where to begin" phasing maps cleanly onto our project trajectory. The §5.2 list of explicit non-achievements ("HANA-class analytical performance is not achievable on Postgres"; "cross-shard transactions are not transactional") is honest in a way the industry survey is not — it knows what it's giving up.

**Caveats:** the proposal assumes a **GL-first** worldview where inventory is one of many subledgers feeding a unified posting-line store. Our codebase took the opposite path — inventory was the first concern, the GL emerged from `transfers`. The proposal does not directly address how to converge a working inventory-first codebase to its design; that's the gap this synthesis fills.

### §2.4 Internal coherence verdict

The three documents are **mutually reinforcing**:
- The cost-methods doc explicitly takes the ledger-architecture's `posting_lines` shape as given and shows how each cost method maps onto it.
- Both the cost-methods and ledger-architecture docs cite the ERP survey as the source of the "what real ERPs do" claims — they are not independently inferring from market research.
- The ledger-architecture proposal cites the same vendor sources (SAP Help Portal, Microsoft Learn, NetSuite Help, Oracle Cloud docs) as the survey.

**One minor tension** noted (and resolved by the proposal): the ERP survey calls D365's posting-layer a clean parallel-accounting mechanism; the cost-methods doc and the proposal both observe it cannot carry **different amounts** per layer (only **inclusion** differs). The proposal therefore retains *both* `ledger_id` (parallel ledgers, different amounts) *and* `posting_layer` (inclusion bitmask), explicitly to recover the strengths of both vendor models. The synthesis adopts the proposal's resolution.

**What the documents do not cover:** RBAC / segregation-of-duties, customer-credit and vendor-prepayment workflows, FX revaluation operational patterns, project / job-cost capitalization paths, and statutory-disclosure-driven LIFO reserve mechanics. These are flagged as future-phase or out-of-scope. We have parallel `bd` issues for several (acct-mqi8 RBAC, acct-8gn customer-credit, acct-3dz2 FX-reval) but they were filed without this research as backdrop — see §10 for the cross-walk.

---

## §3 Current architecture in one page

State as of `d9cd9ee` (2026-05-06): 101 migrations, 647 tests, ci-check clean.

| Concern | Shape | Migration / function reference |
|---|---|---|
| Universal posting-line store | `transfers(id, period_id, debit_account_id, credit_account_id, reason, amount, qty, business_date, posted_at, idempotency_key UNIQUE, document_id, sub_priority, ...)` — ONE row per debit/credit pair, append-only via trigger-blocked UPDATE/DELETE | mig 0007 + 0008 + 0030 (qty) + 0033 (consolidation) |
| Account taxonomy | `account_kind` PG enum (46 values) × partition columns (`counterparty_id` for vendor/customer pools, `sku_id` + `location_id` for stock & inv_value, `routing_op_id` for WIP, `currency` for value pools) | mig 0002 + 16 ALTER TYPE additions across 0022..0101 |
| Transfer reasons | `transfer_reason` PG enum (38+ values) | mig 0002 + ~13 ALTER TYPE additions |
| Cost-method dispatch | Registry: `cost_method_strategies(cost_method, event_kind, compute_fn_name, ...)`. Strategies registered: standard / wac_perpetual / wac_periodic / wac_retroactive at `event_kind='outbound'`. EXECUTE-formatted dispatch. | mig 0094 |
| Period-close orchestration | Registry: `close_hooks(hook_fn_name, ordering, result_key)`. Three hooks at ordering 10/20/30: wac_periodic, wac_retroactive, cost_adjust_retroactive | mig 0095 |
| Document-layer wrappers | 33 `post_*` functions covering inflow (po_receipt / ap_bill / ap_payment), conversion (wo_start / op_move / scrap / wo_complete / wo_close_unproduced / eco_approve / osp_*), outflow (so_ship / so_allocate / customer_invoice / ar_payment / customer_return / customer_credit_memo), inventory_adjustment / cost_adjustment / cost_adjustment_retroactive / standard_cost_roll, po_return / vendor_debit_memo | migs 0012 / 0022 / 0028 / 0035 / 0038 / 0079 / 0081 / 0082..0090 |
| Helper functions | 13 internal helpers under `_post_transfers_*` (lock pre-scan, apply event, compute amount, lookup qty account) and `_wo_*` (resolve_bom, explode_bom, emit_bom_lines, apply_reason_for, burden_events_for_op) | various |
| Bi-temporal | `business_date` (DATE) for logical event date / GL period assignment / cost lookups + `posted_at` (TIMESTAMPTZ DEFAULT clock_timestamp()) for audit. `effective_at` / `obsolete_at` on `bom_headers`; `effective_at` on `standard_costs`. | mig 0007 + scattered |
| Multi-currency | `accounts.currency` (CHAR(3)) per value-ledger account; `fx_rates(from, to, rate, effective_at)`; realized-FX accounts (`realized_fx_gain` / `realized_fx_loss` / `fx_clearing`) | mig 0004 + 0006 + 0092 |
| Append-only | Trigger-blocked UPDATE/DELETE on `transfers`; reversals are new transfers | mig 0008 |
| Idempotency | `transfers.idempotency_key UUID UNIQUE`; R6 dual-check (pre- and post-FOR-UPDATE) on entry-point functions | mig 0007 + acct-69p (mig 0039) |
| Outbox | `ledger_outbox(events JSONB, status, ...)` table exists but `post_transfers` is sync-in-transaction; pseudo-sync (LISTEN/NOTIFY shape L) deferred as `acct-c4p` | mig early + acct-0oy |
| Reservations | `inventory_reservations` with status enum `active / allocated / shipped / cancelled / expired`; `reserve_inventory()` function | mig 0009 + 0014 + 0079 |
| Reconciliation | `run_daily_reconciliation()` checks double-entry per (ledger_kind, currency) and reservation over-promise; alerts to `reconciliation_alerts` | mig 0016 |
| BOM / routing | `bom_headers` + `bom_lines` + `absorption_classes` + `engineering_change_orders` + `wo_outputs` + `bom_by_products` + `wo_by_products`. Phantom expansion, alternates, time-phased revisions, runtime-configurable burden taxonomy. | migs 0040..0099 |
| Method-specific subledger tables (FIFO layers / lot rows / serial rows) | **None.** `acct-8gg` / `acct-uze` / `acct-0kz` filed but unstarted. | n/a |
| Multi-ledger / posting layer | **None.** Single ledger; `ledger_kind` enum (`'value'` / `'qty'`) is used to group double-entry validation, not for parallel accounting. | n/a |
| Multi-entity / sharding | **None.** Single `legal_entity_id` implicit. acct-3gzh (multi-entity foundation) and acct-w1v3 (intercompany) filed. | n/a |
| Partitioning | `transfers` not partitioned. acct-e8g (transfers partitioning) deferred, perf-gated. | n/a |
| Class-confusion R1–R7 | Codified in CLAUDE.md "Load-bearing design decisions"; audited per-function in `REVIEW.md` | acct-du2 + acct-rgb / fii / 69e / 69p / smn / rso / 5prc / quca |

**Project posture** (per CLAUDE.md): Postgres-native; not TigerBeetle parity; correctness > performance; baseline before complexity; deferred outbox; standard-cost-only Phase 0 lifted to wac_*-on-WIP through Phase 1; FIFO/lot/serial gated to Phase 2.

---

## §4 Industry reference model

Synthesized from the ERP survey §7. Two pieces: the reference taxonomy of business events (§4.1), and the modern-ERP capability set (§4.2). A more detailed cross-walk against our shipped work is in Appendix A.

### §4.1 Reference taxonomy (12 categories, ~89 events)

The survey's §7.1 organizes "what an ERP must record" into 12 categories with ~89 named events. Verbatim category titles:

| Cat | Title | # events | Our coverage |
|---|---|---|---|
| A | Procure-to-Pay cycle | 10 | Slice A shipped 7/10 (A2/A3/A4/A5/A7/A8/A10 implemented as `post_po_receipt`, `post_ap_bill`, PPV split, `post_ap_payment`, `post_vendor_debit_memo`, vendor-prepayment partial via memo, `post_po_return`). Missing: A1 (purchase commitment register), A6 (cash clearing distinct from cash), A9 (formal prepayment-application workflow distinct from credit memo). |
| B | Order-to-Cash cycle | 10 | Slice C shipped 8/10 (B1 partial via `sales_orders` stub; B2/B3/B5/B7/B8/B9 partial as `post_so_ship`, `post_customer_invoice`, `post_ar_payment`, `post_customer_return`, `post_customer_credit_memo`). Missing: B4 (POS / cash-sale path), B6 (customer deposit liability handling), B10 (finance charge). |
| C | Inventory and Cost of Goods | 13 | 11/13 covered (C1/C2 via inv_adj_expense; C3 via to_release+to_receipt; C5 via post_cost_adjustment + post_standard_cost_roll; C6/C7 via inventory_adjustment; C8 via post_scrap; C9–C11 via post_wo_start/op_move/wo_complete/wo_close_unproduced + variance accounts; C12 via labor_apply / oh_apply / burden_apply). Missing: C4 (cross-company stock transfer — gated on multi-entity), C13 (landed cost allocation). |
| D | Fixed Assets | 12 | **0/12 covered.** Account kinds reserved (none yet) but no document wrappers. Phase 3+. |
| E | Cash and Banking | 6 | E5/E6 partial (FX revaluation accounts exist; reval workflow is acct-3dz2). Missing: E1–E4 (bank account funding, transfers, charges, interest). Phase 3+. |
| F | Period Close and Adjustments | 9 | 6/9 (F1 via direct-INSERT manual journals; F3/F4 via inventory_adjustments + reversal-as-new-transfer; F5 via close hooks; F8 via close_period; F9 via no special handling — retained earnings P&L close not implemented as discrete event). Missing: F2 (recurring journal templates), F6 (intercompany elimination — gated on multi-entity), F7 (CTA via translation — gated on acct-3dz2). |
| G | Revenue Recognition (ASC 606 / IFRS 15) | 6 | **0/6 covered.** No deferred-revenue / contract-asset / performance-obligation modeling. Phase 3+. |
| H | Lease Accounting (ASC 842 / IFRS 16) | 5 | **0/5 covered.** No ROU asset / lease liability. Phase 3+. |
| I | Payroll and Human Capital | 6 | **0/6 covered.** Account kinds reserved (none yet). acct-9e2 (payroll integration) filed P3. |
| J | Tax | 5 | J1/J2 partial (sales_tax_payable account kind shipped; tax legs flow via vendor_bill / customer_invoice). Missing: J3 (formal tax remittance), J4 (use tax / reverse charge), J5 (withholding). acct-rp2 filed P3. |
| K | Intercompany | 5 | **0/5 covered.** acct-w1v3 (intercompany), acct-3gzh (multi-entity foundation) filed P3. |
| L | Statistical and Memo postings | 2 | None. Reservations are state-not-postings; no statistical KPIs / non-financial postings. |

**Coverage summary:** ~32/89 industry events (~36%) have document wrappers shipped or partially shipped; another ~10 are partially modelable via direct `post_transfers` calls. The bulk of missing coverage is in Fixed Assets (D), Revenue Recognition (G), Lease (H), Payroll (I), and Intercompany (K) — all of which the project explicitly defers to Phase 3+.

### §4.2 Modern-ERP capability set (15 capabilities)

The survey's §7.2 enumerates 15 capabilities a modern ERP must support. Each is a yes/no/partial against our shipped code:

| # | Capability | Status | Notes |
|---|---|---|---|
| 1 | Real-time GL with subledger drill-down | partial | GL-impact emerges as `transfers` immediately. Drill-down via document-FK columns (po_line_id, wo_event_id, etc.). No formal "GL Impact" view UI. |
| 2 | Configurable accounting derivation | partial | `cost_method_strategies` registry + `_wo_apply_reason_for` + `bom_lines.absorption_class_id` are *adjacent* to a configuration tier; the rest is plpgsql code. |
| 3 | Parallel accounting under multiple standards (IFRS / GAAP / tax) | **no** | Single ledger; no posting_layer. Drift-action in §5.4. |
| 4 | Multi-currency with FX revaluation and translation | partial | Posting at row-currency works; revaluation workflow is acct-3dz2 P3; CTA / functional-vs-reporting reporting model not built. |
| 5 | Period management and close orchestration | yes | `close_period` + 3 close hooks. Soft-close / lock states not modeled but in scope. |
| 6 | Intercompany handling end-to-end | **no** | Single entity; acct-w1v3 + acct-3gzh filed. |
| 7 | Subledger event-class architecture | partial | We have `transfer_reason` + per-document state machines; not a formal event-class / event-type / lifecycle-state hierarchy à la Oracle SLA. |
| 8 | Revenue recognition under ASC 606 / IFRS 15 | **no** | Phase 3+. |
| 9 | Lease accounting under ASC 842 / IFRS 16 | **no** | Phase 3+. |
| 10 | Cost accounting beyond GL | **yes** | Standard + 3 WAC variants shipped; FIFO / lot / serial deferred (acct-8gg / acct-uze / acct-0kz). Variance accounting is real. |
| 11 | Project / job costing with capitalization | **no** | Phase 3+. |
| 12 | Sub-second drill-back from GL to source | partial | Joinable via document-FK columns; no formal materialized drill-path. |
| 13 | Audit trail and immutability | **yes** | Append-only `transfers` + idempotency keys + R7 audit-snapshot discipline. Strong here. |
| 14 | Custom transaction types and custom GL segments | partial | New `transfer_reason` values are ALTER TYPE adds (still discrete code change). No customer-facing extension framework. |
| 15 | Statistical / non-financial postings | **no** | Reservations are state, not postings. |

**Coverage:** 3/15 fully shipped (5, 10, 13); 7/15 partial; 5/15 not implemented. The five "no" cells are explicit Phase 3+ deferrals — they don't represent surprise gaps; they represent acknowledged future scope.

---

## §5 Dimension-by-dimension comparison

For each of 17 architectural dimensions: current state, research prescription, industry pattern, verdict, effort. Verdicts use:

- **aligned** — shape and substance match research prescription.
- **drift-acceptable** — diverges from research, no current failure mode, no urgency to converge.
- **drift-action** — diverges with a concrete future failure mode; convergence has measurable cost.
- **gap-deferred** — research prescribes, we have not built; deferral is conscious and tracked.
- **gap-action** — research prescribes, we have not built, current work is hampered by absence.
- **overreach** — research prescribes, we will not adopt, with rationale.

Effort estimates: **S** (~1 mig + 1–2 days), **M** (1–3 migs + 1–2 weeks), **L** (4–10 migs + 4+ weeks), **XL** (>10 migs / multi-month / multi-epic).

### §5.1 Single line-item store

**Current state.** `transfers` (mig 0007 + 0030 + 0033) is the universal posting-line store. Every accounting event in the codebase, from PO receipts through WO completes through AR payments, is one or more rows in `transfers`. Schema: `(id BIGSERIAL, period_id, debit_account_id, credit_account_id, reason, amount, qty, business_date, posted_at, idempotency_key, document_id, sub_priority, posted_by)`.

**Research prescription.** Both the ledger-architecture proposal §2.3.1 and the ERP survey §7.3 argue for a single line-item store. The proposal's `posting_lines` is shaped similarly: `(posting_id, line_seq, posting_date, fiscal_year, fiscal_period, event_class, event_type, source_module, legal_entity_id, ledger_id, posting_layer, account_id, amount_functional, currency_functional, rule_set_id, idempotency_key, created_by_user)`. Critical difference: **N rows per posting** (one row per leg of the entry) vs. our **one row per debit/credit pair**.

The N-rows-per-posting shape comes from SAP ACDOCA, where every line of a journal entry is one row. The 1-row-per-pair shape comes from TigerBeetle, where every transfer is one debit-credit pair. ACDOCA-style supports >2 legs per posting natively; TB-style requires multiple `transfers` rows for the same logical event with a shared `document_id` to model a 3+ leg entry (e.g., `post_po_receipt`'s 3-event PPV split: `inv DR / GR DR-or-CR / ap CR`).

**Industry pattern.** All four surveyed ERPs use N-rows-per-posting (SAP `ACDOCA`, Oracle Fusion `XLA_AE_LINES`, D365 `GeneralJournalAccountEntry`, NetSuite `transactionaccountingline`). The TB-style 1-row-per-pair shape is a deliberate departure from industry orthodoxy that was chosen because (a) the v0.1 design was TigerBeetle-parity, (b) inventory is the project's primary concern and per-class qty dispatch is more natural with debit/credit pairing.

**Verdict.** **drift-acceptable.** Our shape works. The proposal's N-rows shape would be cleaner for >2-leg entries (e.g., the 3-leg PPV split, the 4-leg `post_so_ship` shipment with COGS + revenue + tax + AR-staging) but the cost of converging is replacing every `post_*` function and every test, with no correctness gain at our scale. The shape we have **does** preserve double-entry — it preserves it twice, once per row, instead of by group-by-posting-id. Worth flagging in `ledger_design_consolidated_v0.md` as a deliberate divergence.

**Effort to converge.** **XL.** Effectively a rewrite. **Recommendation: do not converge.** Document the divergence; do not reverse it.

### §5.2 GL-aggregate vs. subledger-detail separation

**Current state.** No separation. `transfers` is the single store for every event grain — financial-only events (`post_ar_payment` writing `cash DR / ar CR`) and inventory-detail events (`post_op_move` writing per-class qty + per-class value legs across operation pools) share the same table.

**Research prescription.** The cost-methods doc's central thesis: **GL records aggregate financial impact; subledger records per-unit / per-lot / per-layer detail**. §2.4 calls this "non-optional at scale": *"A high-volume organization with serialized inventory might generate one billion GL lines per year purely to record per-serial detail. The financial reality of that same organization might be 50 million GL lines representing actual financial events. Conflating the two means every period-close report drags through 20× more data than necessary; index sizes balloon; query performance degrades; partition management becomes unwieldy."* Each cost method gets its own subledger table: `inventory_movements`, `moving_average_costs` log, `cost_layers` + `cost_layer_depletions`, `inventory_lots` + `inventory_lot_events`, `inventory_units` + `inventory_unit_events`. The GL `posting_lines` references subledger detail via foreign keys (`cost_layer_id`, `lot_id`, `unit_id`).

**Industry pattern.** Universal. SAP has `MSEG` (material movements) feeding `MLIT` then `ACDOCA`. Oracle Fusion has Cost Management's perpetual cost engine writing detail; XLA writes summary to GL. D365 has `InventTrans` for inventory detail; `GeneralJournalAccountEntry` for GL. NetSuite has item ledger views feeding `transactionaccountingline`.

**Verdict.** **drift-action.** This is the most consequential divergence, and the one that bites first when FIFO/lot/serial cost methods land. Right now, with standard + WAC, the cost-detail surface is small enough that the conflation has no concrete failure mode (a `transfers` row carries `qty` and `amount` and that's enough). When FIFO requires `cost_layers(id, original_qty, unit_cost, ...)` rows that are NOT GL postings, our `transfers`-as-everything model has nowhere to put them. Either (a) we extend `transfers.qty` semantics to point at a layer (research-prescribed approach: GL row references subledger row), or (b) we add a separate `cost_layers` table that exists alongside `transfers` (research-prescribed approach: GL row aggregates over subledger events). The third path — store the layer events in `transfers` with a synthetic reason like `'fifo_layer'` and no GL impact — would force `transfers` to become both posting-line and detail-event storage, exactly the conflation the cost-methods doc warns against.

**Effort to converge.** **L.** New subledger tables + refactor of dispatcher to read from them are net new code (~3–5 migs); FIFO/lot/serial document wrappers are already on the backlog (acct-8gg / acct-uze / acct-0kz), this is the architectural shape they'd take.

**Recommendation:** **before** starting `acct-8gg`, write a load-bearing-decision update to `ledger_design_consolidated_v0.md` answering: "GL-aggregate / subledger-detail split — do we, or do we not?" If yes, the next `bd` issue under `acct-8gg` is "introduce `inventory_movements` foundational subledger" + a follow-up to backfill existing `transfers` rows (or deliberately not backfill — the table goes live for new methods only). If no, the dispatcher's FIFO branch reads layer state from `transfers` itself filtered by reason / document type, and we accept the future scaling risk.

### §5.3 Account taxonomy

**Current state.** PG enum `account_kind` (46 values across mig 0002 + 16 ALTER TYPE additions); per-account-kind partitioning columns on `accounts` (`counterparty_id` for vendor/customer pools, `sku_id` + `location_id` + `currency` for value pools, etc.).

**Research prescription.** Both the proposal §2.3 and industry treat `accounts` as a row table referenced by FK. The proposal: *"`account_id` as the only inline account reference. Earlier designs had `account_natural` denormalized; dropped — the analytical replica joins to `accounts`."* Account properties (postable / open-item / effective-from / effective-to / status) live as columns on the `accounts` row, not as code. Industry uses chart-of-accounts: usually 4–8 hierarchical segments (entity / division / department / natural account / project / location / etc.).

**Industry pattern.** SAP T030 / OBYC for account determination; Oracle Fusion `gl_code_combinations`; D365 `MainAccount` + flexible dimensions; NetSuite item/entity assignment + chart of accounts.

**Verdict.** **drift-action**, but acct-2thf (account_kind enum → row table) is already filed. The current shape works at 46 enum values; it gets unwieldy past ~80 (a generally accepted threshold; the enum was originally going to be a row table per acct-2thf rationale). The drift here interacts with §5.4 (multi-ledger / posting-layer) and §5.13 (dimensions / segments) — fixing the enum is the natural moment to introduce a chart-of-accounts hierarchy and flex-dimension columns.

**Effort to converge.** **M.** New `account_kinds` row table + lookup-by-string replacement of enum literal in plpgsql + new ALTER on `accounts` to add a foreign key + `_post_transfers_lock_pre_scan` and similar helpers updated to use `account_kind_id` instead of the enum literal. Roughly 2 migrations + a sweep across the plpgsql surface.

**Recommendation:** lift **acct-2thf** from "threshold-gated" (we said 80 values; we're at 46) to "do it now". The threshold was always a soft signal; the architectural drift is real and the cost is moderate. Phase 2.

### §5.4 Multi-ledger / posting-layer

**Current state.** Single ledger; no `posting_layer`. The `ledger_kind` enum (`'value'` / `'qty'`) is used for double-entry validation grouping (sum credits = sum debits per ledger_kind per currency), not for parallel accounting.

**Research prescription.** Both the proposal §2.4.1 and industry universally support parallel accounting. The proposal's resolution is **two columns**:
- `ledger_id SMALLINT NOT NULL` — distinguishes parallel ledgers SAP-style (0L = leading IFRS, 2L = US GAAP, 3L = tax). Each ledger receives complete postings; an event relevant to two ledgers produces two posting-line rows with potentially different amounts (e.g., different depreciation methods).
- `posting_layer INT NOT NULL` — D365-style bitmask indicating which "views" a single line participates in. Layers don't duplicate rows; they tag inclusion.

The proposal explicitly retains both because they answer different questions: parallel ledgers handle situations where the *amount* differs by accounting standard; posting layers handle situations where the *inclusion* differs.

**Industry pattern.** SAP parallel-ledger + extension-ledger + Universal Parallel Accounting (UPA); Oracle Fusion primary + secondary + reporting ledgers driven by SLA; D365 ten posting layers (Current, Operations, Tax, Custom 1–7) [verified[1]]; NetSuite Multi-Book Accounting (paid module).

**Verdict.** **gap-action** for `posting_layer` (cheap to add, future-proof, capability gain); **drift-acceptable** for `ledger_id` (we are single-ledger today, and full parallel ledgers means duplicating posts under different rules — substantial work). Adding `posting_layer` now does NOT commit us to parallel ledgers; it just gives us a tag column whose default value preserves all current behavior.

**Effort to converge.** `posting_layer` column on `transfers`: **S** (1 mig, default 1, add to indexes, no behavioral change). Full parallel-ledger via `ledger_id`: **L** (rules engine has to evaluate per ledger; close hooks have to run per ledger; reconciliation has to balance per ledger).

**Recommendation:** add `posting_layer` to `transfers` and `accounts` now (Phase 2, P3). Defer parallel-ledger via `ledger_id` until a customer requirement (IFRS-vs-local-GAAP, dual-cost-book, etc.) actually surfaces (Phase 3+). Surface this in §11 as an open question: do we want parallel ledgers in scope at all? If no, even `posting_layer` may be unnecessary — but it's so cheap that adding it as future-proofing has near-zero cost.

### §5.5 Cost-method dispatch architecture

**Current state.** Registry-driven. `cost_method_strategies(cost_method, event_kind, compute_fn_name, flag_provisional, ...)` maps `(method, event_kind)` to a per-strategy plpgsql function. The dispatcher (`_post_transfers_compute_amount`) does qty-NULL gate + R2 credit-first SKU resolution then EXECUTE-formats the registered function. Adding FIFO/lot is INSERT into the registry + write the function. Mig 0094.

**Research prescription.** The proposal's §3.2 Tier 1 configuration tier is exactly this shape, made more general: rule sets with effective dates, journal-line rules with conditional firing, account derivation through mapping sets, dimension assignment rules. Configuration as the primary mechanism; code as the escape hatch.

**Industry pattern.** Oracle Fusion's SLA is the gold standard (event-class → event-type → accounting-method → journal-line-rule → account-rule). SAP's account-determination tables (T030, OBYC) are configuration-based. D365 posting profiles + posting definitions. NetSuite item/entity assignments + SuiteGL plug-ins.

**Verdict.** **aligned**, with a caveat. Our registry handles cost dispatch and close-hook ordering; it does not handle **account derivation** (we hardcode `cogs DR / inv_value_fg CR` patterns in plpgsql, vs. the industry pattern of "look up the cogs account via mapping set"). This is the next layer of research-alignment — and the natural moment to lift it is when account_kind → row table lifts (§5.3).

**Effort to converge** further (account-derivation tier): **L.** Substantial refactor to introduce a `journal_line_rules` table and replace per-`post_*`-function event construction with rule evaluation. Probably overkill for our single-tenant scope.

**Recommendation:** the registry pattern (mig 0094 + 0095) was the right move; keep extending it (e.g., for the next costing method, register the strategy rather than hardcoding the dispatch). **Do not adopt** Tier 2 WASM (§8). Defer account-derivation rule layer until a concrete need surfaces (multi-tenant? customer-customizable? not currently in scope).

### §5.6 Drill-back / source linkage

**Current state.** Per-document FK columns on transfers' source documents (po_line_id, wo_event_id, so_line_id, vendor_bill_id, etc.). `transfers.document_id` UUID. Audit-trail traceability via joins. No materialized `posting_line_sources` table.

**Research prescription.** Proposal §2.3.4: dedicated `posting_line_sources(posting_id, line_seq, posting_date, source_doc_type, source_doc_id, source_doc_line, source_doc_external_ref, reverses_line_id, parent_posting_id, intercompany_pair_id, custom_module_hash, created_by_process)` extension. Centralizes drill-back to one table; supports reversal-chain traceability via `reverses_line_id`.

**Industry pattern.** SAP ACDOCA carries source-document fields inline. Oracle Fusion XLA tables carry source linkage. D365 GeneralJournalAccountEntry has `OriginalDocument`. NetSuite transactions self-reference via `createdfrom`.

**Verdict.** **drift-acceptable.** Our per-document-FK approach is functional; drill-back works via SQL joins. The proposal's centralized extension table buys uniform UI ("show me where this posting came from" in one query) but not correctness. The cost of converging is low (a new partitioned table + populating from existing FKs), but the value is also low without a UI tier.

**Effort to converge.** **M.** Create the table; backfill from existing per-document-FK columns; add `posted_at` denormalization for partition alignment; switch reads to it. Estimated 2 migs + a backfill.

**Recommendation:** defer until a UI / API tier exists. Until then, the per-document FKs are sufficient and the centralized table is busywork.

### §5.7 Bi-temporal modeling

**Current state.** `transfers.business_date` (logical event date, used for GL period assignment + cost lookups via `resolve_standard_cost_at`) + `transfers.posted_at` (TIMESTAMPTZ DEFAULT clock_timestamp(), audit). Document tables have `business_date` + `posted_at`. Cost tables have `effective_at` + `posted_at`. BOM has `effective_at` + `obsolete_at`.

**Research prescription.** Proposal §4.3: `posting_date` (effective time) + `created_at` (transaction time). Reports default to effective time; audit/restatement uses transaction time. *"This discipline must be encoded somewhere — in policies, in code, in user training. The schema supports it; the schema does not enforce it."*

**Industry pattern.** Universal: SAP `BUDAT` (posting date) + `CPUDT` (entry date); Oracle Fusion `accounting_date` + `creation_date`; D365 `TransDate` + `CreatedDateTime`; NetSuite `trandate` + `lastmodifieddate`.

**Verdict.** **aligned** on schema; **gap-deferred** on rule-table effective dating. The proposal's rules engine has effective-dated rule sets (a rule changed April 15 → an event posting back to March 10 evaluates against the rule effective March 10). Our `cost_method_strategies` registry is *not* effective-dated — it's a hot-loaded mutable registry. If we ever change a strategy mid-period, replays would use the new strategy retroactively. This isn't currently a concern (changes are migrations, which are global events) but is a future-rule-engine consideration.

**Effort to align further.** **S–M.** Add `effective_from` + `effective_to` columns to `cost_method_strategies` and `close_hooks`; the dispatcher and close-period orchestration switch to "find active strategy at business_date" lookups.

**Recommendation:** add effective-dating to the two registries when next touched (likely when adding FIFO/lot/serial); not urgent.

### §5.8 Append-only enforcement

**Current state.** `transfers` UPDATE/DELETE blocked via BEFORE trigger (mig 0008). Reversals are new transfers. Idempotency via `transfers.idempotency_key UUID UNIQUE`.

**Research prescription.** Proposal §2.6: dual enforcement — (1) PostgreSQL role grants (`REVOKE UPDATE, DELETE`), (2) trigger-level RAISE EXCEPTION. *"Reversals as new rows."* The cost-methods doc §2.5: subledger inherits append-only.

**Industry pattern.** Universal among modern ERPs (SAP audit-trail, D365 reversal documents, NetSuite transaction modifications via reversals + new transactions, Oracle Fusion immutable accounting events).

**Verdict.** **aligned**, with one tightening opportunity: we have the trigger-level enforcement; we don't have the role-based enforcement. The application connects as a role with full UPDATE/DELETE permissions and the trigger is the only thing stopping it. Defense-in-depth would be `REVOKE UPDATE, DELETE ON transfers FROM acct;` and a separate admin role for partition management / archival.

**Effort to align further.** **S.** One migration to introduce role grants; potential coordination cost to ensure the dev / test environments work. Optional but cheap.

**Recommendation:** add role-based enforcement when RBAC work (acct-mqi8) starts; not urgent on its own.

### §5.9 Outbox / async patterns

**Current state.** `ledger_outbox` table exists but unused on the hot path. `post_transfers` is sync-in-transaction. acct-c4p (pseudo-sync via LISTEN/NOTIFY shape L) is the documented escape hatch, deferred until measured contention surfaces.

**Research prescription.** Proposal §4.1: outbox pattern is foundational. Operational subledger writes an `accounting_events` row in the same transaction as the operational record; worker process drains via `FOR UPDATE SKIP LOCKED`; rules engine evaluates and persists postings; status transitions (`pending → processing → posted`/`failed`/`skipped`). Trade-off: events not posted instantaneously.

**Industry pattern.** SAP's posting pipeline is configurably sync or async per document type; Oracle Fusion's "Create Accounting" + "Post Journals" are async by design; D365 has both real-time and batch posting; NetSuite is real-time at the source record.

**Verdict.** **gap-deferred.** We chose synchronous explicitly (CLAUDE.md "Sync `post_transfers` for now; pivot to pseudo-sync (shape L) deferred to Phase 1+"). The deferral is operational, not architectural — the infrastructure for shape L (DrainConfig.notify_channel, single-listener dispatcher, drain-tx pg_notify with SQLSTATE payload) is already built and benched.

**Effort to converge.** **L.** Rebuild every test fixture around an async-listener-rendezvous call site; rewire every `post_*` function to enqueue rather than sync-post; build worker process, deploy, monitor. Tracked as `acct-c4p`.

**Recommendation:** stay deferred. Revisit when Phase 1 produces measured contention, per existing CLAUDE.md decision. The synthesis adds no urgency.

### §5.10 Multi-currency

**Current state.** `accounts.currency` (CHAR(3)) per value-ledger account; `fx_rates(from, to, rate, effective_at)` table; realized-FX accounts (`realized_fx_gain` / `realized_fx_loss` / `fx_clearing`) shipped mig 0092; FX revaluation workflow deferred as `acct-3dz2`. Documents carry per-line `currency`.

**Research prescription.** Proposal §4.5: functional currency (legal entity property), transaction currency (event property), group currency (consolidation hierarchy property), local statutory (optional fourth). FX rate types: spot / period-average / period-end / historical. Realized vs unrealized FX. *"Currency conversion is monotonic. Once a row's amounts in functional/group/local are computed and persisted, they are never changed."*

**Industry pattern.** Universal. Differences are in granularity (SAP's parallel-currency at material-ledger level via UPA; Oracle Fusion's reporting-currency translation per ledger; D365's accounting-currency / reporting-currency / transaction-currency split; NetSuite OneWorld functional-vs-elimination-vs-reporting).

**Verdict.** **aligned-but-shallow.** Our shape supports transaction-currency posting; we lack functional-currency-vs-reporting-currency reporting. Realized-FX accounting machinery exists (mig 0092); reval workflow is the gap. acct-3dz2 (FX revaluation foundation) and acct-3xcg (realized FX) cover this.

**Effort to align further.** **M.** Reval workflow + functional-currency reporting model. acct-3dz2 / acct-3xcg are the existing trackers.

**Recommendation:** keep existing trajectory; no synthesis-driven change.

### §5.11 Period-close orchestration

**Current state.** `close_period(period_id, actor, force_provisional, force_recon)` (mig 0026) + `close_hooks` registry (mig 0095) with three hooks at ordering 10/20/30 (wac_periodic, wac_retroactive, cost_adjust_retroactive). FOR UPDATE on the periods row for serialization. P0014/P0015/P0016 gates.

**Research prescription.** Proposal §4.6: subledger close before GL close; period-end processes (depreciation, accruals, allocations, FX revaluation) idempotent and replayable; reconciliation runs before close-confirmation; close itself is a database operation (insert into `closed_periods`); state machine with soft_closed / closed / locked / reopened; year-end carryforward as a posting.

**Industry pattern.** All four ERPs have close orchestration; cloud-era products (NetSuite, D365) have stronger checklist tooling than SAP S/4HANA core (which historically relied on Financial Closing Cockpit).

**Verdict.** **aligned** on shape and execution. We have the registry; we have the gates; we have the actor parameter. Missing: state machine richer than `closed_at IS NULL / NOT NULL` (no soft-close vs lock distinction), no year-end carryforward as discrete event (P&L close to retained earnings is not modeled).

**Effort to align further.** **M.** State enum on periods (`status ∈ open/soft_closed/closed/locked/reopened`); year-end-close hook posting P&L→retained earnings.

**Recommendation:** lift state machine to multi-state when `acct-7h4` (period reopen workflow) starts — it's the natural moment. Year-end carryforward is gated on retained-earnings concept, which is an open-questions item (we don't model retained earnings explicitly; it's implicit in the running balance).

### §5.12 Reconciliation

**Current state.** `run_daily_reconciliation()` (mig 0016) checks (1) double-entry per (ledger_kind, currency), (2) reservation over-promise; alerts to `reconciliation_alerts` table. Runs daily via pg_cron.

**Research prescription.** Proposal §4.7: reconciliation as a first-class concern. Subledger-to-control-account reconciliations daily; cross-shard intercompany reconciliations; tier-1-vs-tier-2 rule output reconciliations during migration; replication-lag monitoring; rule-rule consistency. *"The ratio of reconciliation engineering to feature engineering is higher than most teams expect."*

**Industry pattern.** SAP "FAGLB04" reconciliation programs; Oracle Account Reconciliation Cloud; D365 ledger reconciliation; NetSuite balance comparisons. All four maintain control-account semantics that the research formalizes.

**Verdict.** **aligned-but-shallow.** Our recon scope is narrow (double-entry + reservation). The proposal's broader scope (subledger-sum = GL-aggregate, intercompany match, rule-rule consistency) lifts naturally as features arrive. **gap-deferred** on subledger-vs-control-account once subledger separation lands (§5.2).

**Effort to broaden.** Per-feature; not a single migration. Subledger reconciliation = automatic if §5.2 is adopted.

**Recommendation:** add reconciliation per new subledger as it ships. No standalone rework needed.

### §5.13 Dimensions / segments

**Current state.** Implicit per-event dimensions via `account_kind` + `counterparty_id` + `currency` + `sku_id` + `location_id` + `routing_op_id`. No explicit cost-center, profit-center, project, or department dimensions.

**Research prescription.** Proposal §2.3.2: dedicated `posting_line_dimensions(posting_id, line_seq, posting_date, dimension_type, dimension_value)` extension table — EAV-typed with rows only when dimension is populated. `dimension_types` lookup table for type names. Industry chart-of-accounts has 4–8 segment types.

**Industry pattern.** Multi-segment chart-of-accounts is universal. SAP `ACDOCA` has cost-center + profit-center + project + segment + assignment columns; Oracle Fusion `gl_code_combinations` has 5–9 segments; D365 financial dimensions are unlimited; NetSuite has subsidiary + class + department + location + custom-segments.

**Verdict.** **drift-acceptable** at our scale; **gap-action** when (a) multi-entity lands (acct-3gzh), (b) cost-center reporting becomes needed, (c) projects with capitalization paths land. The drift here interacts with §5.3 — adding the dimensions extension makes most sense alongside the account_kind→row-table refactor.

**Effort to converge.** **M–L.** New extension table + UI / report wiring + populating from existing context (the `_post_transfers_apply_event` function would set dimensions from caller-supplied event context).

**Recommendation:** lift when acct-3gzh starts. The single-entity codebase doesn't need dimensions yet.

### §5.14 Idempotency

**Current state.** `transfers.idempotency_key UUID UNIQUE`; R6 dual-check (pre- and post-FOR-UPDATE) on entry-point functions; `*.idempotency_key UNIQUE` on every document table.

**Research prescription.** Proposal §4.2: three layers — source-event idempotency, posting idempotency, rule evaluation idempotency. The three compose so retrying any operation at any point produces the same final state.

**Industry pattern.** SAP idempotency via document numbers + source references; Oracle Fusion via "transfer-to-GL" mode that's exactly-once; D365 voucher numbers; NetSuite external-id deduplication.

**Verdict.** **aligned** + **strong**. The R6 dual-check is more disciplined than most production systems. We've shipped acct-69p as a load-bearing rule.

**Effort to align further.** None.

**Recommendation:** keep doing what we're doing. This is a confirmed-correct cell.

### §5.15 Partitioning / sharding

**Current state.** `transfers` not partitioned. acct-e8g (transfers partitioning) deferred, perf-gated. Single shard.

**Research prescription.** Proposal §2.5: monthly partitioning by `posting_date` on the core and every extension; sharding by `legal_entity_id`; BRIN indexes on `posting_date` (kilobytes for billions of rows, append-friendly); B-tree on `(account_id, fiscal_year, fiscal_period)` and `(ledger_id, account_id, posting_date)` and `(event_class, posting_date)`.

**Industry pattern.** SAP HANA handles by columnar storage (no partitioning needed at HANA's scale — column compression is the equivalent); Oracle Fusion partitions by ledger and accounting period; D365 partitions by company + period; NetSuite has account-period-based partitioning internally.

**Verdict.** **gap-deferred**. acct-e8g deliberately gated on perf-baseline justification. The deferral logic still holds: we don't have measured pressure.

**Effort to converge.** **L.** Convert `transfers` to a partitioned table (data motion + index rebuild + partition-management pg_cron). acct-e8g is the tracker.

**Recommendation:** keep deferred. Lift when Phase 1 perf measurements show partition-pruning would help (likely not before serialized inventory or 100M-row regimes).

### §5.16 Method-specific subledger tables

**Current state.** None. FIFO `cost_layers`, lot `inventory_lots`, serial `inventory_units` — all absent. Standard / WAC fit in `transfers` because their per-class state is reconstructable from `transfers.qty` + `transfers.amount` running sums.

**Research prescription.** The cost-methods doc dedicates §§4–11 to method-specific subledger schemas. FIFO requires `cost_layers + cost_layer_depletions`; lot requires `inventory_lots + inventory_lot_events`; serial requires `inventory_units + inventory_unit_events`. The volume table (§15.1) shows serialized scenarios produce 25–50× the GL row count. *"Conflating the two means every period-close report drags through 20× more data than necessary."*

**Industry pattern.** All four ERPs have these. SAP `MSEG` / `MCH1` / `EQUI`; Oracle Fusion Cost Management cost-flow tables; D365 `InventTransOriginCostLayer`; NetSuite item ledger.

**Verdict.** **gap-deferred**, with the §5.2 caveat: when these methods land (acct-8gg / acct-uze / acct-0kz), the architectural decision about subledger separation is forced. Defer the decision; do not defer the awareness that it must be made.

**Effort to converge.** **L per method.** Each is a new table family + dispatcher branch + close-hook (where periodic) + reconciliation invariant.

**Recommendation:** sequence per the cost-methods doc's §16 — Phase 1 standard + WAC done; Phase 2 FIFO/LIFO; Phase 3 actual costing layer; Phase 4 lot + serial. Our `bd` ordering matches but we should explicitly link `acct-8gg` blocked-by the §5.2 decision.

### §5.17 Reciprocal drill-back ergonomics

**Current state.** Caller-side joins between `transfers` and document tables. No formal "GL impact" UI / view.

**Research prescription.** Proposal + ERP survey §7.3: *"Visible GL impact at the operational record."* NetSuite's GL-Impact link on every record is the gold standard.

**Industry pattern.** NetSuite GL-Impact link on every transaction; SAP `FB03` document display + ACDOCA-direct-drill; D365 voucher detail; Oracle Fusion subledger-to-GL drill.

**Verdict.** **gap-deferred** — the reciprocal drill exists structurally (joinable via FK columns); the UI tier doesn't exist; not in current scope.

**Effort to converge.** **M** when an API/UI tier ships. Until then, n/a.

**Recommendation:** punt. Resurfaces when an API/UI tier is in scope.

---

## §6 Where we've gone astray

Three drift-action / gap-action items rank by severity. (Distilled from §5.)

### §6.1 (drift-action) `transfers` is both the GL-posting-line store AND the inventory-event-grain store

**Severity: high (latent).** No current failure mode at standard + WAC scale. Forces an architectural decision the moment FIFO / lot / serial cost methods land (§5.2, §5.16). The decision belongs in `ledger_design_consolidated_v0.md`, not in the FIFO migration's body.

**Concrete failure mode:** when `cost_layers(id, original_qty, unit_cost, ...)` are layer-events that need to be append-only, indexable by `(product, location, receipt_date)`, depletable by ORDER BY receipt_date, and reconcilable against GL-aggregated COGS — the natural shape is a separate table. The natural alternative is to overload `transfers.qty` and `transfers.amount` with synthetic reasons (`'fifo_layer_create'`, `'fifo_layer_deplete'`) that have no GL impact (NULL idempotency_key? amount=0? both feel wrong). Either choice is consequential; making the choice silently inside the FIFO migration would be worse than choosing deliberately first.

### §6.2 (gap-action) `acct-2thf` (account_kind enum → row table) gated on a soft threshold

**Severity: medium.** The enum-driven account taxonomy is a known-bad-idea at growth (every new account_kind is a migration, can't be customer-configured, can't be parameterized). We're at 46 enum values with a stated "lift when 80" threshold. The threshold was always a soft signal, not a structural argument. The structural argument is: industry uses chart-of-accounts row tables; we don't; cost is moderate; do it.

**Concrete consequence of delay:** acct-3gzh (multi-entity), acct-w1v3 (intercompany), and acct-v9sq (chart-of-accounts hierarchy) all touch this. If we ship multi-entity before lifting acct-2thf, the multi-entity migrations either inherit the enum constraint (multi-entity per-enum-value duplication) or have to re-do the per-entity account setup post-lift. Sequence pain.

### §6.3 (gap-action) Single ledger; no `posting_layer` dimension

**Severity: low (potential).** Adding `posting_layer SMALLINT NOT NULL DEFAULT 1` to `transfers` and `accounts` is a one-migration cost with zero behavioral change. Not adding it means parallel-accounting work later (acct-w1v3 intercompany, acct-3gzh multi-entity, eventual IFRS-vs-local-GAAP) starts from a non-multi-ledger schema and has to retrofit the column on a billion-row table.

**Concrete consequence:** posting_layer adds bitmask filtering that's natural in industry; not having it means future "show me only IFRS-tagged postings" queries have no path. Cost-of-delay is a future migration on a much larger `transfers`.

### §6.4 (drift-action) `transfers`'s row-per-pair shape (vs. row-per-leg)

**Severity: low (acknowledged).** This is the §5.1 dimension. We're row-per-debit-credit-pair (TigerBeetle style); industry is row-per-leg (ACDOCA style). The 3-leg PPV split and the 4-leg `post_so_ship` shipment use multiple rows with a shared `document_id`. It works; double-entry is preserved (twice, redundantly). Documenting it as a deliberate divergence in `ledger_design_consolidated_v0.md` removes future-contributor confusion.

**Concrete consequence of leaving undocumented:** future contributors reading the `posting_lines` proposal and seeing our `transfers` may "fix" the row-per-pair to row-per-leg, breaking every test and every dispatcher branch. This is preventable with a load-bearing-decision note.

---

## §7 Where we hold up

Six confirmed-correct decisions. This section tells future-us "stop re-litigating these."

### §7.1 Append-only enforcement (§5.8)
Trigger-blocked UPDATE/DELETE on `transfers`. Reversal-as-new-row pattern. Industry-universal; the proposal confirms; the cost-methods doc confirms. **No change.**

### §7.2 Idempotency (§5.14)
`transfers.idempotency_key UNIQUE` + R6 dual-check is more disciplined than the proposal's spec. We have a stronger invariant than industry. **No change.**

### §7.3 Period-close orchestration with registry-driven hooks (§5.11)
`close_period` + `close_hooks` registry is the industry shape, with state-machine richness as the only future enhancement. **No change.**

### §7.4 Bi-temporal at row grain (§5.7)
`business_date` + `posted_at` aligned with proposal. Effective-dating on configuration tables is a future tightening, not a current gap. **No change.**

### §7.5 Multi-currency at row grain (§5.10)
Per-account currency + per-document currency + FX rates table + realized-FX accounts. Reval workflow is the gap, tracked by acct-3dz2; the foundation is correct. **No change.**

### §7.6 Registry-driven cost-method dispatch (§5.5)
mig 0094's `cost_method_strategies` + EXECUTE-formatted dispatch is the shape the research advocates. Adding FIFO is INSERT into the registry + write the function. **No change.** (Strengthen by extending the same shape to account-derivation when a need surfaces.)

---

## §8 The unified target

What we should converge on. References §5 throughout. Deliberately calls out things we will *not* adopt.

### §8.1 What we will adopt

**Keep `transfers` as the universal posting-line store.** Don't rename; don't restructure. The shape works. Document the deliberate divergence (row-per-pair vs row-per-leg, §5.1) in `ledger_design_consolidated_v0.md`.

**Add `posting_layer SMALLINT NOT NULL DEFAULT 1` to `transfers` and `accounts`** (§5.4). Cheap; future-proof; zero behavioral change. Index `(posting_layer, business_date)` for future filtered reporting.

**Add `legal_entity_id SMALLINT NOT NULL DEFAULT 1` to `transfers` and `accounts`** (§3 Top-5 item 5, §5.4). Single-entity preserved; future multi-entity (acct-3gzh) gets a free zero-migration foundation.

**Lift `acct-2thf` (account_kind enum → row table)** before multi-entity (§5.3). Cost is moderate; the enum threshold was always soft.

**Adopt subledger separation when FIFO/lot/serial cost methods land** (§5.2, §5.16). Per-method subledger tables with `posting_id` + `posting_line_seq` FKs back to `transfers`. The `inventory_movements` foundational table (proposal-prescribed shape) becomes the umbrella; `cost_layers` / `cost_layer_depletions` / `inventory_lots` / `inventory_lot_events` / `inventory_units` / `inventory_unit_events` extend as needed. **`transfers` stays GL-aggregate; subledger carries detail.**

**Extend the registry pattern** (mig 0094 / 0095) to new domains as they arise — cost-method × event-kind, close-hook ordering, future event-class × ledger × layer rules. The proposal's Tier-1 configuration tier is a generalization of what we've already started.

**Deepen reconciliation as subledgers ship** (§5.12). Each subledger gets a recon invariant against its GL control account. Add the recon at the same time as the subledger, not as a follow-up.

### §8.2 What we will not adopt

**Tier-2 WASM rules engine.** Reasons:
- Our project is single-tenant; the customization surface that WASM addresses (customer-deployed sandboxed modules) does not exist for us.
- Our plpgsql strategy registry (mig 0094) plays the customization role today, and "the customization surface IS plpgsql" is a coherent answer for our scale.
- Tier-2 testing infrastructure is "30–50% of the work of building tier 2" per the proposal §3.3.5; the maintenance burden exceeds value at our scale.
- WASM cold-start performance, host-API stability commitments, source-archive retention, shadow-mode-vs-production-replay tooling — all multi-quarter engineering investments without a customer driver.
- The proposal itself flags this: *"some platforms will be better served by a simpler approach (configuration only, with tier 2 as a future phase) until they have the engineering capacity and customer base to support both tiers well."*

**ACDOCA-shape wide-row table.** Reasons:
- Postgres row-store penalizes wide rows; the proposal explicitly argues against ACDOCA-on-Postgres.
- We have `transfers` working narrow already; converging to wide-row would be net harm.

**Full parallel-ledger duplication via `ledger_id`** (yet). Adding `posting_layer` is cheap; adding parallel-ledger amount-divergence is expensive. Defer until a concrete IFRS-vs-local-GAAP requirement surfaces. Surfaces in §11 as an open question.

**Industry GL Impact UI as Phase 1+ work.** No API / UI tier exists; the reciprocal drill is functional via SQL joins. Punt to when an API tier ships.

**Sharding-by-legal-entity until acct-3gzh** (multi-entity foundation) actually justifies it. Single-entity codebase doesn't need shards.

### §8.3 The shape of the converged system

A one-paragraph picture. After convergence:

> `transfers` remains the universal posting-line store, append-only, idempotent, bi-temporal, with per-row `posting_layer` and `legal_entity_id` for future parallel accounting and multi-entity. Cost methods that need per-unit / per-lot / per-layer detail (FIFO, lot, serial — Phase 2+) write subledger detail to dedicated tables (`cost_layers`, `inventory_lots`, `inventory_units`) with FKs back to `transfers`; the dispatcher reads subledger state for cost computation and writes subledger events alongside GL events under the same idempotency umbrella. Account taxonomy is a row table (`account_kinds` + `accounts` referencing it), with chart-of-accounts dimensions (cost-center / profit-center / project) added via a `posting_line_dimensions` extension when needed. The cost-method-strategy registry (mig 0094) and close-hooks registry (mig 0095) generalize to a Tier-1 rules engine over time as account-derivation and event-class registration patterns surface; we will not build Tier-2 WASM. Sync `post_transfers` remains, with the LISTEN/NOTIFY shape-L escape hatch (acct-c4p) as documented; if measured contention surfaces in Phase 2, we pivot. Multi-entity (acct-3gzh) and intercompany (acct-w1v3) are built on the `legal_entity_id` foundation laid early. Parallel-ledger amount-divergence stays deferred until a concrete IFRS / local GAAP requirement justifies it.

---

## §9 Migration roadmap

Phased convergence. Each phase is gated on its preconditions; phases A and B are safe to do now; C is gated on the next costing method.

### §9.A Free-and-clear additive (Phase 2, P3)

Cheap, additive, zero behavioral change. Do these first to lay foundations for everything else.

- **A1.** Add `posting_layer SMALLINT NOT NULL DEFAULT 1` to `transfers` and `accounts`. Index `(posting_layer, business_date)`. 1 migration. **S**.
- **A2.** Add `legal_entity_id SMALLINT NOT NULL DEFAULT 1` to `transfers` and `accounts`. 1 migration. **S**.
- **A3.** Document deliberate divergence: row-per-pair vs row-per-leg in `ledger_design_consolidated_v0.md` Part IV §1; cross-link from CLAUDE.md "Load-bearing design decisions". **S**.
- **A4.** Lift `acct-2thf` (account_kind enum → row table). 2 migrations + plpgsql sweep. Establishes foundation for chart-of-accounts segments. **M**.

### §9.B Decision-forced moments (Phase 2, P2)

Architectural decisions that need to be answered before the next blocked epic moves.

- **B1.** Settle the GL-aggregate / subledger-detail decision in `ledger_design_consolidated_v0.md` (§6.1). Path A (subledger separation) or Path B (everything in `transfers` with synthetic reasons). Phase 2 work that is NOT a migration; just a decision document. **S decision; M-L downstream**.
- **B2.** Reaffirm Tier-2 WASM scoping decision: not adopted, with §8.2 rationale captured in CLAUDE.md or `ledger_design_consolidated_v0.md`. **S**.
- **B3.** Decide whether parallel-ledger amount-divergence is in Phase 3 scope. If yes: add `ledger_id SMALLINT NOT NULL DEFAULT 0` alongside `posting_layer`; otherwise: hold off. **S decision**.

### §9.C Gated on next cost method (Phase 2 / 3)

Triggered when `acct-8gg` (FIFO/LIFO) or `acct-uze` (lot) or `acct-0kz` (serial) starts, contingent on B1's outcome.

- **C1 (if Path A).** Introduce `inventory_movements` foundational subledger (proposal §4.3 shape). All real-cost methods write here in addition to `transfers`. Reconciliation: SUM(inventory_movements.amount) over a period equals SUM(transfers.amount) where account_id is `inv_value_*`. **L**.
- **C2 (if Path A).** Add method-specific subledger tables per the cost-methods doc §§4–11: `cost_layers + cost_layer_depletions` (FIFO/LIFO); `inventory_lots + inventory_lot_events` (lot); `inventory_units + inventory_unit_events` (serial). Each is a new migration set + dispatcher branch + close-hook (where periodic) + recon invariant. **L per method.**
- **C3 (either path).** Add `posting_line_sources`-style centralized linkage table only when an API / UI tier ships. Until then, per-document FKs are sufficient. **Defer**.

### §9.D Gated on multi-entity (Phase 3)

Triggered when `acct-3gzh` (multi-entity foundation) starts, leveraging A2's prelaid `legal_entity_id`.

- **D1.** Lift `legal_entity_id` from "always 1" to a real foreign key + RBAC scoping. Reservations, recon, close all become entity-scoped. **L**.
- **D2.** Implement intercompany sagas (acct-w1v3) with `intercompany_pair_id` UUID column on `transfers` (or a `posting_line_sources`-style extension). Cross-entity recon. **L**.
- **D3.** Implement chart-of-accounts segments (`posting_line_dimensions` extension) with cost-center / profit-center / department / project as standard dimensions. Per-account dimension defaulting via mapping sets. **L**.

### §9.E Gated on real perf pressure (deferred indefinitely)

- **E1.** Lift `acct-e8g` (transfers partitioning) when measured Phase-2 contention or row-count justifies it. Until then, BRIN on `business_date` is sufficient.
- **E2.** Lift `acct-c4p` (pseudo-sync via shape L) when measured contention surfaces.
- **E3.** Sharding by legal_entity (proposal §2.5.2) as a multi-cluster topology — when entity volume justifies it. Probably never for this project.

---

## §10 Issue catalog

`bd`-ready candidates. Format: `### Title (Pn) [bd-id-if-existing]` followed by Why / Scope / Blocks-on / Effort / References. Issues are **draft prep, not yet filed** — actual `bd create` invocations are a downstream session.

Cross-reference column: existing-bd ids are cited verbatim so a future tooling pass can match them.

### §10.1 Immediate / unblock-existing (P2 / P3)

#### Add posting_layer column (P3, NEW)
**Why:** future-proof for IFRS / local GAAP / tax-basis / management-adjustment tagging without a `transfers` rewrite later. Cost is one migration; benefit is preserved optionality.
**Scope:** ALTER TABLE transfers ADD COLUMN posting_layer SMALLINT NOT NULL DEFAULT 1; ALTER TABLE accounts likewise; index (posting_layer, business_date) on transfers; document semantics in CLAUDE.md.
**Blocks-on:** none.
**Effort:** S.
**References:** synthesis §5.4 / §6.3 / §9.A1.

#### Add legal_entity_id column (P3, NEW)
**Why:** zero-cost prelaid foundation for future multi-entity work (acct-3gzh). Avoids forced migration on a much larger transfers table later.
**Scope:** ALTER TABLE transfers / accounts ADD COLUMN legal_entity_id SMALLINT NOT NULL DEFAULT 1; index where needed.
**Blocks-on:** none.
**Effort:** S.
**References:** synthesis §5.4 / §6.3 / §9.A2.

#### Document `transfers` row-per-pair-shape divergence (P3, NEW)
**Why:** prevent future contributors from "fixing" the TigerBeetle-style row-per-pair to ACDOCA-style row-per-leg.
**Scope:** add a load-bearing-decision paragraph to `ledger_design_consolidated_v0.md` Part IV §1; cross-link from CLAUDE.md.
**Blocks-on:** none.
**Effort:** S.
**References:** synthesis §5.1 / §6.4 / §9.A3.

#### Lift acct-2thf (account_kind enum → row table) from soft-threshold to "do it now" (P2, [acct-2thf])
**Why:** structural drift, not threshold-driven. Lifting before multi-entity prevents sequence pain.
**Scope:** new account_kinds row table; replace enum literal with FK lookup in plpgsql; sweep across `_post_transfers_*` and `post_*` functions.
**Blocks-on:** synthesis §6.2 decision (likely none).
**Blocks:** acct-3gzh / acct-w1v3 / acct-v9sq / acct-3uh.
**Effort:** M.
**References:** synthesis §5.3 / §6.2 / §9.A4.

#### GL-aggregate / subledger-detail load-bearing decision (P2, NEW)
**Why:** the next costing method (acct-8gg / acct-uze / acct-0kz) forces this decision; making it deliberately first prevents accidental drift inside a feature migration.
**Scope:** decide Path A (subledger separation) vs Path B (synthetic-reason rows in transfers). Update `ledger_design_consolidated_v0.md` Part IV with the decision and rationale. No code change.
**Blocks-on:** synthesis review by user.
**Blocks:** acct-8gg, acct-uze, acct-0kz.
**Effort:** S (decision doc).
**References:** synthesis §5.2 / §5.16 / §6.1 / §9.B1.

#### Reaffirm Tier-2 WASM out-of-scope (P3, NEW)
**Why:** prevent the next architectural-research cycle from re-asking "should we add WASM" without context.
**Scope:** add §8.2 rationale to CLAUDE.md or design doc; cite the proposal's own §3.5 caveat.
**Blocks-on:** none.
**Effort:** S.
**References:** synthesis §8.2 / §9.B2.

### §10.2 Short-term Phase 2 deliverables (P3)

#### Effective-dating on cost_method_strategies and close_hooks (P3, NEW)
**Why:** the registries are not effective-dated; mid-period strategy changes would replay retroactively. Not an active concern but a future-rule-engine prereq.
**Scope:** add effective_from / effective_to columns; dispatcher and close_period select active row at business_date.
**Blocks-on:** none (could ship anytime).
**Effort:** S–M.
**References:** synthesis §5.7.

#### Subledger reconciliation per new cost method (P2, NEW; one issue per method)
**Why:** reconciliation invariant per §5.12. Run as part of run_daily_reconciliation; alert via reconciliation_alerts.
**Scope:** SUM(subledger.amount) = SUM(transfers.amount on inv_value_*) per (sku, location, period); alert on divergence.
**Blocks-on:** the cost method's subledger landing.
**Effort:** S per method.
**References:** synthesis §5.12.

#### Year-end carryforward as discrete event (P3, NEW)
**Why:** F9 in the industry taxonomy (§4.1); not currently modeled as a discrete event. Not blocking but a real ERP capability.
**Scope:** new `post_year_end_close` document wrapper; close P&L accounts to `retained_earnings`; introduce account_kind 'retained_earnings' if needed.
**Blocks-on:** acct-2thf (cleaner with row-table accounts).
**Effort:** M.
**References:** synthesis §5.11 / Appendix A F9.

#### Lift period state machine from boolean to enum (P3, partially [acct-7h4])
**Why:** soft_closed / closed / locked / reopened states per the proposal §4.6.
**Scope:** ALTER TABLE periods; add status enum; refactor close_period gating.
**Blocks-on:** acct-7h4 (period reopen workflow).
**Blocks:** acct-7h4 implementation.
**Effort:** M.
**References:** synthesis §5.11.

#### Add `posting_line_sources`-style audit table (P4, NEW)
**Why:** centralized drill-back; mostly UI-tier-driven, low priority until UI tier exists.
**Scope:** new partitioned table; populate from per-document FKs; switch reads.
**Blocks-on:** API/UI tier work (not currently filed).
**Effort:** M.
**References:** synthesis §5.6.

### §10.3 Long-term Phase 3+ (P3 / P4)

#### Parallel-ledger via ledger_id (P3, decision pending)
**Why:** if IFRS / local-GAAP / tax-basis dual reporting is in scope.
**Scope:** add ledger_id column; replicate posting per ledger; per-ledger close hooks; per-ledger recon.
**Blocks-on:** §9.B3 decision.
**Effort:** L.
**References:** synthesis §5.4 / §11 open question.

#### Industry GL-Impact UI / drill-down (P4, gated on API tier)
**Why:** modern-ERP capability #1, #12. Not Phase 1+ until an API tier exists.
**Scope:** API endpoints + UI views; reciprocal source ↔ GL drill.
**Blocks-on:** API tier (not currently filed).
**Effort:** XL.
**References:** synthesis §5.17 / Appendix A.

#### Multi-currency reporting model (functional vs reporting currency) (P3, [acct-3dz2])
**Why:** ERP capability #4 partial; reval shape exists, reporting model doesn't.
**Scope:** functional currency per legal entity; reporting currency translation hook; CTA accounting.
**Blocks-on:** acct-3xcg (realized FX), legal_entity_id column.
**Effort:** L.
**References:** synthesis §5.10 / §4.2 cap. 4.

### §10.4 Cross-walk to existing bd backlog

| Existing bd id | Synthesis section | Verdict |
|---|---|---|
| acct-8gg (FIFO/LIFO/lot cost methods) | §5.2 / §5.16 / §6.1 / §10.1 | **Now blocked-by** the GL-aggregate / subledger-detail decision. Add explicit dependency. |
| acct-uze (lot tracking) | §5.16 / §6.1 | Same. |
| acct-0kz (serialized / catch-weight) | §5.16 / §6.1 | Same. |
| acct-2thf (account_kind enum → row table) | §5.3 / §6.2 / §10.1 | **Lift from threshold-gated to immediate**. |
| acct-3gzh (multi-entity foundation) | §5.4 / §5.13 / §9.D | Sequence after legal_entity_id column + acct-2thf. |
| acct-w1v3 (intercompany) | §5.4 / §9.D2 | Sequence after acct-3gzh. |
| acct-v9sq (chart-of-accounts hierarchy) | §5.13 | Sequence after acct-2thf; gates on dimensions extension. |
| acct-3dz2 (FX revaluation foundation) | §5.10 / §4.2 cap. 4 / §10.3 | No synthesis-driven change; on existing trajectory. |
| acct-3xcg (realized FX) | §5.10 / §10.3 | No synthesis-driven change. |
| acct-c4p (pseudo-sync shape L revisit) | §5.9 / §9.E2 | No synthesis-driven change; perf-gated. |
| acct-e8g (transfers partitioning) | §5.15 / §9.E1 | No synthesis-driven change; perf-gated. |
| acct-7h4 (period reopen workflow) | §5.11 / §10.2 | Add period-state-enum lift as a sub-issue or dependency. |
| acct-mqi8 (RBAC) | §5.8 / §4.2 | No synthesis-driven change; would naturally introduce role-based append-only enforcement when it ships. |
| acct-i86d (approval workflow framework) | §4.2 cap. 14 | No synthesis-driven change. |
| acct-cw6b (transfer orders), acct-3uh (OSP physical custody) | §5.13 / §4.1 C3-C4 | No synthesis-driven change; both filed. |
| acct-9ij (negative inventory) | §5 (not specifically covered) | No synthesis-driven change; cost-methods doc §5.5 / §14.4 confirms "configure per item" is the right pattern. |
| acct-cms (alternate provisional cost sources for wac_periodic) | §5.5 | Aligned with proposal's effective-dated rule pattern. |
| acct-7t4 (by-products epic — closed) | n/a | already shipped 2026-05-06. |
| acct-nnyl (WAC-parents + by-products) | §5.5 | filed P3; confirmed-correct shape. |

### §10.5 Net new issues this synthesis surfaces

Five issues from §10.1 (posting_layer, legal_entity_id, document divergence, GL/subledger decision, reaffirm WASM-out-of-scope). Plus §10.2's effective-dating, year-end-close, period-state-enum, audit-table, subledger-recon-per-method.

---

## §11 Open questions for the user

These are genuine judgment calls that don't have a single right answer; the synthesis surfaces them rather than deciding them.

### §11.1 Parallel-ledger scope

Is IFRS / local-GAAP / tax-basis parallel reporting in scope at all for this project? If yes, the `posting_layer` column suffices for inclusion-only divergence; full parallel ledgers via `ledger_id` are a Phase-3 commitment. If no, even `posting_layer` may be unnecessary — but it's so cheap that adding it as future-proofing has near-zero cost.

**Synthesis recommendation:** add `posting_layer` regardless; defer `ledger_id` until a concrete dual-reporting requirement surfaces.

### §11.2 GL-aggregate / subledger-detail decision (§9.B1)

The next costing method forces the choice. Path A (industry-prescribed subledger separation) vs Path B (synthetic-reason rows in `transfers`). Path A is more work upfront but scales correctly to serialized inventory; Path B is the lower-friction extension of what we already have but accumulates a future scaling debt.

**Synthesis recommendation:** Path A. The cost-methods doc's §2.4 argument ("non-optional at scale") is structurally compelling. We'd rather pay the architectural cost now with FIFO than retrofit it later under serialization pressure.

### §11.3 Multi-tenant ambitions

The Tier-2 WASM tier exists for customer-deployed sandboxed customization in multi-tenant SaaS ERPs. Are we ever going to be that? If the answer is "no, we're a single-tenant project", then §8.2's WASM-out-of-scope decision is permanent. If the answer is "maybe eventually", then we should at least keep the door open with a rule-evaluation indirection layer.

**Synthesis recommendation:** assume single-tenant unless told otherwise. Keep WASM out of scope.

### §11.4 Where the consolidated design doc lives

The synthesis surfaces several recommended updates to `ledger_design_consolidated_v0.md` (row-per-pair divergence; subledger-detail decision; WASM out-of-scope; posting_layer addition). These are **changes to the design doc**, not the synthesis. Should the user (a) take the synthesis as input and update the design doc separately; or (b) ask the synthesis to draft the design-doc edits as proposed-text patches? The plan defaults to (a); (b) is available if preferred.

### §11.5 What to do with `research/` after synthesis

The three research files served their purpose. After synthesis lands, options are: (a) keep them as historical input for future readers; (b) move to ARCHIVE/ alongside the predecessor docs; (c) delete (they're in `git log`). The synthesis itself stays in `research/`.

**Synthesis recommendation:** option (a) for now; reconsider in 6 months.

---

## §12 Appendix A: §7.1 cross-walk

The 12-category × ~89-event reference taxonomy from the ERP survey, mapped to our `transfer_reason` enum and `post_*` document wrappers. **Coverage = 1 if shipped or partially shipped via `post_transfers` direct call; 0 if not.**

### A. Procure-to-Pay (10)
| # | Event | Coverage | Reason / wrapper |
|---|---|---|---|
| A1 | Purchase commitment created | 0 | `purchase_orders` exists as document; no commitment register |
| A2 | Goods physically received | 1 | `post_po_receipt` → `po_receipt` |
| A3 | Vendor invoice received | 1 | `post_ap_bill` → `ap_bill` |
| A4 | Vendor invoice price differs from PO | 1 | PPV split inside `post_po_receipt` and `post_ap_bill`; reasons `ppv` / `variance_match_tolerance` |
| A5 | Payment issued to vendor | 1 | `post_ap_payment` → `ap_payment` |
| A6 | Payment cleared at bank | 0 | No cash-clearing distinct from cash |
| A7 | Vendor credit / debit memo | 1 | `post_vendor_debit_memo` (financial + goods-return); `post_po_return` |
| A8 | Prepayment to vendor | 0 | No formal prepayment workflow |
| A9 | Prepayment applied | 0 | No formal prepayment-application workflow (acct-8gn filed) |
| A10 | Goods returned to vendor | 1 | `post_po_return` → `po_return_to_vendor` (with PPV reversal symmetric) |

### B. Order-to-Cash (10)
| # | Event | Coverage | Reason / wrapper |
|---|---|---|---|
| B1 | Sales commitment created | 1 (partial) | `sales_orders` + `post_so_allocate` |
| B2 | Goods shipped to customer | 1 | `post_so_ship` → `so_ship` |
| B3 | Customer invoice issued | 1 | `post_customer_invoice` → `ar_invoice` |
| B4 | POS / cash sale | 0 | No combined cash+ship+invoice path |
| B5 | Customer payment received | 1 | `post_ar_payment` → `ar_payment` |
| B6 | Customer deposit / on-account | 0 | acct-8gn filed |
| B7 | Customer credit memo / refund | 1 | `post_customer_credit_memo` (financial); `post_customer_return` (goods) |
| B8 | Customer goods returned | 1 | `post_customer_return` → `customer_return` |
| B9 | Bad debt write-off | 0 | No distinct workflow |
| B10 | Late charge / finance charge | 0 | acct-33v6 filed (aging dunning) |

### C. Inventory and Cost of Goods (13)
| # | Event | Coverage | Reason / wrapper |
|---|---|---|---|
| C1 | Inventory consumption to cost center | 1 | `post_inventory_adjustment` against `inv_adj_expense` |
| C2 | Inventory consumption to project / WBS | 0 | No project segment yet |
| C3 | Inter-warehouse transfer | 1 (partial) | `to_release` / `to_receipt` reasons; no `post_to_*` wrapper (acct-cw6b filed) |
| C4 | Cross-company stock transfer | 0 | gated on multi-entity (acct-3gzh) |
| C5 | Inventory revaluation | 1 | `post_cost_adjustment` + `post_standard_cost_roll` (with WIP reval option) |
| C6 | Physical shrinkage | 1 | `post_inventory_adjustment` (negative qty) → `inv_adj_expense` (acct-hzan filed for cycle-count workflow) |
| C7 | Physical overage | 1 | symmetric (positive qty) |
| C8 | Scrap | 1 | `post_scrap` → `scrap` + `scrap_v` |
| C9 | Production: components issued | 1 | `post_wo_start` + `post_op_move` (rm_issue_to_wo) |
| C10 | Production: finished goods received | 1 | `post_wo_complete` → `wo_complete` + `wo_complete_v` |
| C11 | Production variance | 1 | `wo_close_v`, `op_move_v`, plus 4 WAC variants and `variance_yield_byproduct` |
| C12 | Overhead absorption | 1 | `labor_apply` / `oh_apply` / `burden_apply` reasons |
| C13 | Landed cost allocation | 0 | not modeled |

### D. Fixed Assets (12)
**0/12 covered.** Reserved account_kinds (none yet); no document wrappers. Phase 3+ epic; no specific bd id beyond placeholder.

### E. Cash and Banking (6)
| # | Event | Coverage | Notes |
|---|---|---|---|
| E1 | Bank account funded / drawn | 0 | No bank-account modeling |
| E2 | Bank-to-bank transfer | 0 | acct-i2qz (bank recon) filed |
| E3 | Bank charges | 0 | |
| E4 | Bank interest | 0 | |
| E5 | FX revaluation | 1 (partial) | Realized-FX accounts shipped; reval workflow acct-3dz2 |
| E6 | FX realized at payment | 1 (partial) | acct-3xcg filed |

### F. Period Close and Adjustments (9)
| # | Event | Coverage | Notes |
|---|---|---|---|
| F1 | Manual journal | 1 | Direct INSERT-and-call-post_transfers possible |
| F2 | Recurring journal | 0 | No template engine |
| F3 | Accrual / provision | 1 (partial) | `post_inventory_adjustment` for inv accruals; no general accrual workflow (acct-6q8f filed) |
| F4 | Reversal of accrual | 1 | reversal-as-new-transfer pattern |
| F5 | Allocation | 1 (partial) | close hooks via registry |
| F6 | Intercompany elimination | 0 | gated on multi-entity |
| F7 | Currency translation | 0 | gated on acct-3dz2 |
| F8 | Period close (lock) | 1 | `close_period` + close_hooks registry |
| F9 | Year-end carryforward | 0 | no retained-earnings concept (P3 — see §10.2) |

### G. Revenue Recognition (6)
**0/6 covered.** Phase 3+.

### H. Lease Accounting (5)
**0/5 covered.** Phase 3+.

### I. Payroll and Human Capital (6)
**0/6 covered.** acct-9e2 filed P3.

### J. Tax (5)
| # | Event | Coverage | Notes |
|---|---|---|---|
| J1 | Output tax on sales | 1 (partial) | `sales_tax_payable` account; tax legs in customer_invoice |
| J2 | Input tax on purchases | 1 (partial) | tax legs in vendor_bill |
| J3 | Tax remittance | 0 | acct-rp2 filed |
| J4 | Use tax / reverse charge | 0 | |
| J5 | Withholding tax | 0 | |

### K. Intercompany (5)
**0/5 covered.** acct-w1v3 / acct-3gzh filed.

### L. Statistical / Memo (2)
**0/2 covered.** Reservations are state, not postings. Statistical accounts not modeled.

**Cross-walk summary:** 32/89 industry events (~36%) have shipped or partially-shipped wrappers. Bulk of missing coverage is FA / RevRec / Lease / Payroll / IC — all explicit Phase 3+ deferrals.

---

## §13 Appendix B: footnotes and source list

[1] D365 F&O has 10 posting layers: Current, Operations, Tax, plus Custom 1–7 (and a "None" pseudo-layer used for fixed-asset books that don't post to GL). Verified 2026-05-06 via Microsoft Learn ("Post fixed asset transactions to posting layers", https://learn.microsoft.com/en-us/dynamics365/finance/fixed-assets/post-fixed-asset-transactions-posting-layers) and community write-up "Posting Layers in D365FO" (LinkedIn).

[2] SAP S/4HANA's ACDOCA Universal Journal consolidates several predecessor tables (verified 2026-05-06 via SAP Community blog "All you need to know about Universal Journal(ACDOCA) - SAP S/4 HANA (2020)" and SAP-PRESS blog "What Is SAP's Universal Journal?"): ACDOCA captures the line items previously stored in COEP (Controlling), ANEP/ANEA/ANLP/ANLC (Asset Accounting), MLIT (Material Ledger), GLT0/FAGLFLEXA (FI), and others. **Caveat to research-doc claim:** the research document states ACDOCA "consolidates the line items previously stored in BSEG (FI)..."; in practice BSEG (line-item table for FI documents) is **not** fully replaced — it persists in S/4HANA for specific operational finance processes including open-item management. The synthesis treats this as a minor inaccuracy in the research doc that doesn't change downstream conclusions.

[3] Other industry claims surfaced in the source documents (e.g., NetSuite Group Average uniqueness; ASC 330 / IAS 2 NRV requirement; LIFO Conformity Rule; Wasmtime cold-start latency; BRIN index size scaling; specific Oracle SLA event-class counts; SuiteGL launch year; ACDOCA introduction year and ~350-column count) are cited from the source research files but **not independently web-verified for this synthesis**. Where any such claim drives a recommendation, the synthesis flags the dependency rather than asserting the claim. Future work: a follow-up verification pass (~half-day) before the synthesis is treated as decision-grade rather than discussion-grade.

[4] Source documents cited inline: `research/erp-transaction-architectures-revised.md` (1,039 lines, 2026 vintage); `research/cost-methods-subledger-design.md` (2,126 lines); `research/ledger-architecture-proposal.md` (1,584 lines).

[5] Project-internal references: CLAUDE.md "Load-bearing design decisions"; `ledger_design_consolidated_v0.md` Part IV §1–§7; REVIEW.md Phase 1 + Phase 2 audit + R1–R7 anti-pattern catalog; migrations 0001..0101.

[6] `bd` issue ids cross-referenced in §10 are valid as of 2026-05-06; backlog state was 38 ready P3 issues plus 1 deferred (acct-vli) at the time of writing.

---

**End of synthesis v0.** Length: ~1,950 lines including tables. Targeted range was 1,500–2,500. The §1 executive summary and §10 issue catalog are the load-bearing reads; §5 is the substantive comparison; §11 surfaces what the user should weigh in on; the rest is reference material for future-self and contributors.
