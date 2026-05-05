# ERP Research Synthesis — Phase 2 Backlog vs Industry Standard

**Compiled 2026-05-05** against repo `/home/kaalin/dev/acct` at migration 0090. Source research: `/tmp/erp_research_group{1..6}_*.md`.

Coverage: 27 open P3 backlog issues compared across **SAP S/4HANA**, **Oracle Cloud SCM/Financials/Fusion (and EBS)**, **Microsoft Dynamics 365 F&O**, and **NetSuite**. Per issue: industry approaches, pros/cons, common extensions beyond the four, our concrete gaps, and a recommendation for when picked up.

---

## Executive synthesis — six cross-cutting patterns

Walking the four ERPs against our 27 issues, the same architectural shapes recur:

1. **Three-state period model is universal.** Open / Soft-closed / Permanently-closed. F&O makes it most explicit; SAP, Oracle, NetSuite all converge on the same lattice. Cascade-on-reopen is consensus. Our binary `closed_at IS NULL` is the only outlier — and the per-call `p_override_closed_period` we added on returns is an awkward midpoint that should retire when a real reopen workflow lands.

2. **Strict-by-default with explicit per-item / per-org overrides.** Every ERP ships negative inventory off, FX reval as a separate program, period reopen permission-gated, lot/serial as item flags. The override is a config decision, not a runtime bypass. We currently mix runtime bypasses (`p_force_provisional`, `p_override_closed_period`) with absent gates — the right shape is per-resource policy flags + a separate workflow.

3. **Sub-ledger / staging separation is universal.** GRNI (`ap_unsettled`), unbilled receivables (`ar_unsettled`), customer deposits, vendor advances, special stock at vendor — every mainstream ERP keeps these in dedicated GL accounts rather than letting cleared-AP/AR go negative. Our `ap_unsettled` / `ar_unsettled` / `variance_*` pattern is on the right track; the rest of the AR/AP credit-balance / refund / OSP custody work extends it cleanly.

4. **Native engine + certified partner is the answer for sales tax (and increasingly payroll).** All four ERPs ship a native rules engine for simple cases AND a certified Vertex/Avalara/ONESOURCE connector for complex jurisdictional rules. None go pure-external; none go pure-internal. Hybrid wins on day-one and on long-tail rate maintenance both.

5. **Async transport per document type, not globally.** SAP V1/V2 update tasks, Oracle AQ + Workflow Background Engine, F&O batch framework, NetSuite Map/Reduce — all four separate "doc accepted" from "GL posted" *for some doc types*, never globally. Our shape L (LISTEN/NOTIFY pseudo-sync) is mainstream pattern, not exotic. Pivot per doc-type, not all-at-once.

6. **Translate-at-posting beats multi-currency-per-row, except in SAP Material Ledger.** Oracle, D365, NetSuite all snapshot home-currency cost at receipt/issue; SAP's three parallel currencies (local/group/profit-center) is heavyweight and only mid-to-large enterprises light it up. For our cross-currency BOM (acct-tt1) and FX revaluation (acct-3dz2), translate-at-posting is the right MVP shape.

**One regression-style finding to flag**: our class-confusion checklist R1–R6 (CLAUDE.md) maps directly onto patterns the four ERPs implicitly enforce. R3 (solo-occupancy gating) is exactly SAP's "drain cost center to zero only when sole producer" pattern in CKMLCP / KSII. R5 (single-leg variance on debit-normal pools drained in-period) is exactly Oracle's Average Cost Variance. R6 (idempotency dual-check) is exactly SAP's enqueue-then-row-lock pattern. Our naming was independent but the underlying invariants are identical — useful for code review confidence when these issues land.

---

# Group A — Cost methods & costing infrastructure

The cost-method picture across the four ERPs:

| | Standard | WAC perpetual | WAC periodic | FIFO | LIFO | Lot/specific |
|---|---|---|---|---|---|---|
| SAP S/4HANA | yes (`S`) | yes (`V` MAP) | via Material Ledger PUP | via AVR (post-hoc) | via AVR | batch valuation (split valuation) |
| Oracle Fusion Cloud | yes | yes | PAC (project SC) | yes (true layers) | **no** | actual-cost orgs |
| MS Dynamics 365 F&O | yes | yes (running avg) | settle at close | yes (post-hoc) | yes (post-hoc) | "Financial inventory" on dim |
| NetSuite | yes | yes (default) | n/a | yes | yes (not AU) | Lot Numbered / Specific |

**Where we sit**: standard + WAC trio (perpetual / periodic / retroactive) shipped end-to-end through migrations 0029–0070. FIFO and lot raise P0006.

## acct-8gg — Cost methods beyond standard (FIFO / LIFO / lot)

**Problem**: `cost_method` enum already declares `'fifo'` and `'lot'` but every dispatch in `_post_transfers_compute_amount` raises P0006.

**Industry**:
- **SAP**: Material Ledger reconstructs FIFO/LIFO at PUP run via Alternative Valuation Runs (AVR); no real-time layers. Lot-specific = batch valuation (`BWTAR`).
- **Oracle Fusion**: Per-receipt layer rows consumed FIFO; LIFO **not supported** (MOS 2798743.1). Lot tracking is separate from costing; specific costing requires actual-cost orgs.
- **D365 F&O**: All non-standard methods are *periodic-style* — post at running average, settle at inventory close. "Financial inventory" flag on lot/serial dimension forces close-time per-(sku, lot) settlement = effective specific costing.
- **NetSuite**: Broadest method menu (Average / FIFO / LIFO / Lot Numbered / Specific / Group Average / Standard). Method **immutable** once set on the item.

**Pros/Cons**:
- SAP is most rigorous (full quantity structure preserved) but heaviest implementation.
- Oracle is most explicit about layer mechanics — real per-receipt layers consumed in order; no LIFO is a real gap.
- D365 has the worst auditability (issue costs change retroactively at close) but the most flexible mid-period.
- NetSuite has only true per-lot/per-serial costing of the four; immutability is the audit trade-off.

**Our gap**: No layer schema. No lot/serial dimension. Existing `(sku, location, currency, kind)` pool key would extend naturally to `(... lot_id)` once acct-uze adds lots.

**Recommendation**:
- Adopt **Oracle Fusion's explicit-layer model** for FIFO (`inventory_layers(sku_id, location_id, kind, currency, receipt_id, layer_qty, layer_unit_cost, sequence_no)`).
- Treat **lot as a pool dimension** (extend pool key) rather than a parallel cost method — aligns with our existing kind-based partitioning.
- Stage after WIP-on-WAC stabilizes; the layer ↔ WIP interaction needs careful R4 (FOR-UPDATE-on-same-account) design.
- LIFO: skip unless a regulatory case demands it (Oracle's stance is informative).

## acct-cms — Alternate provisional cost sources for wac_periodic / wac_retroactive

**Problem**: Hardcoded running-avg in `wac_periodic_close_hook` (mig 0029). Real ERPs let operators pick the provisional source.

**Industry**:
- **SAP**: "Release prior PUP as new standard" makes the *next* period's mid-period valuation = prior period's actual. Cleanest model — close-time variance shrinks period-over-period.
- **Oracle Fusion**: Cost-profile-driven — per-transaction-type rules. Most extensible, configuration-heavy.
- **D365**: "Include physical value" flag on the model group is the most flexible per-process knob and closest to our GRNI staging shape.
- **NetSuite**: Running average only. No knob.

**Industry extensions**: Epicor / Infor LN offer "configured zero" for engineer-to-order; process ERPs (Process Pro, BatchMaster) use last-known-good batch cost.

**Our gap**: No `provisional_cost_source` enum on `skus` or per-pool config. Hook walks `transfers_provisional` filtered by cost_method in a single recompute branch.

**Recommendation**:
- Add `skus.provisional_cost_source TEXT` with values `running_avg` (default), `last_period_close_avg`, `last_purchase_price`, `standard`, `configured`, `zero`.
- Extend `_post_transfers_compute_amount`'s wac_periodic / wac_retroactive branches to switch on the field. R2 applies (credit-side SKU resolution).
- Stage *after* acct-c4p so we don't double-rewrite the dispatcher.

## acct-obw — Capacity-based costing (hours × rate decomposition)

**Problem**: `bom_lines.std_amount` is single pre-rolled BIGINT. No hours/rate decomposition → no labor-efficiency variance, no capacity-utilization variance.

**Industry**:
- **SAP** (gold standard): CO-PC activity types (`LABOR_HRS`, `MACHINE_HRS`, `SETUP_HRS`) per cost center; KP26 plan prices (fixed + variable); routing specifies hours per activity type per work center; postings flow through secondary cost elements (type 43). Capacity-utilization variance is structural.
- **Oracle Fusion**: Resource rates + Overhead rates on Work Definition; date-effective; hourly or per-unit. Most pragmatic.
- **D365**: Cost categories per operation resource + cost groups for type classification. Setup / process / quantity time can each have own cost category — most useful for detailed routing analysis.
- **NetSuite**: Manufacturing Cost Templates with Labor Run / Machine Setup / Machine Run + per-base overhead. Coarsest.

**Industry extensions**: Plex uses real-time machine telemetry (OEE) for dynamic rates; IFS does fixed/variable rate split per work center.

**Our gap**: No resources / work centers / activity types / rate cards. `wo_routings(wo_id, routing_op, op_name)` is text-only. `variance_muv / lv / ohv` enums reserved but unwired.

**Recommendation**:
- Adopt **D365's cost-categories shape**: `cost_categories(id, code, kind ∈ {labor, machine, overhead, setup}, default_rate)` + `routing_operations(routing_id, op_no, cost_category_id, hours_per_unit, hours_per_lot)`.
- Layer **Oracle's date-effective rates** on top: `cost_category_rates(category_id, rate, effective_at)` — composable with `bom_header_at` time-phased pattern.
- Defer SAP-style activity-type / cost-center modeling.
- R3 (solo-occupancy) and R5 (single-leg variance) apply on revaluation paths.

## acct-ijt — Time-phased absorption rates

**Problem**: `std_amount` is immutable per BOM revision; rates can't transition cleanly for in-flight WOs.

**Industry**:
- **SAP**: KP26 rates period-versioned per fiscal year; KSII / KSRU revalue activity prices for in-flight orders. Marking + release is period-bounded.
- **Oracle Fusion**: Date-effective rate rows with `start_date / end_date`. Operations completed at the rate effective on transaction date — natural rate-pickup. Cleanest fit for Postgres-native.
- **D365**: Costing versions (Pending → Active) overlap with effective dates; year-boundary standard-cost conversion revalues WIP.
- **NetSuite**: Manual Standard Cost Adjustment documents — coarsest, weakest.

**Our gap**: `bom_lines.std_amount` immutable. `absorption_classes` has no rate field. No per-class analogue of `resolve_standard_cost_at`.

**Recommendation**:
- Sibling to `standard_costs`: `absorption_class_rates(class_id, rate, effective_at, posted_by)` — append-only, mirrors mig 0027.
- `resolve_absorption_rate_at(p_class_id, p_business_date) RETURNS BIGINT` — canonical lookup, raises P0018-style.
- Refactor `_wo_emit_bom_lines` to compute amount as `qty_per_parent × p_qty × resolve_absorption_rate_at(class_id, business_date)` on opted-in lines.
- Reuse acct-bru's `p_revalue_wip` revaluation primitive for rate transitions on in-flight WOs against new `variance_absorption_revaluation`.
- **Coordinate with acct-obw** — capacity-based costing is the prerequisite for meaningful rate time-phasing.

---

# Group B — Manufacturing / BOM2 extensions

Our BOM2 (acct-jg2, migrations 0040–0062) has solid bones: header/lines, alternate_no, ECO scaffolding, phantom expansion, fire_at semantics, dual-mode yield. The seven manufacturing extensions in this group all attach cleanly to BOM2 seams.

## acct-c80 — Per-op planned yield + variance reporting

**Problem**: BOM2 has per-line `scrap_pct` and per-parent `yield_mode`. No per-routing-op yield, no planned-vs-actual yield variance reporter.

**Industry**:
- **SAP**: Operation yield first-class on routing master; CO11N captures yield + scrap + rework qty. Variance buckets named (price, lot-size, qty, scrap).
- **Oracle Cloud**: 24C added "Operation Yield" on Work Definition; cost rollup scales components by `1/yield` cumulatively across routing. Yield Variance = (actual reported − planned scaled) × std cost.
- **D365**: Both BOM/formula scrap_pct and route-level scrap; route scrap accumulates as `Accumulated% = NextOp_Accumulate% × 100/(100 − scrap_pct)`.
- **NetSuite**: Reporting-grade only — no automatic standard-cost rollup uplift from per-op yield.

**Our gap**: `wo_routings` has no yield column. `skus.yield_mode` is binary (`plan_only` vs `absorbed`) at parent-rollup level only. Variance only as `variance_wo_close` residual.

**Recommendation**:
- `wo_routings.planned_yield_pct NUMERIC(5,2)`. At rollup time, `yield_mode='absorbed'` parents inflate parent_std by cumulative `1/(1-scrap_pct)` per op; `plan_only` records yield without inflation.
- Materialize per-op actual yield from `wo_events` (op_move qty_in vs qty_out + scrap_v at op). Activate `variance_muv`; emit `op_yield_v` reason at op_move when actual ≠ planned.
- Defer rolled-throughput-yield reporting to a Tier-2 mat view.

## acct-7t4 — By-products: NRV credit-back / negligible / disposal-cost

**Problem**: `wo_outputs` supports co-products via `allocation_pct`; missing the three classic by-product treatments.

**Industry**:
- **SAP**: CO-PC distinguishes joint products / co-products (apportionment structure) from by-products (NRV: `cost_main = total_cost − Σ NRV_byprod`). Negative NRV (disposal) supported as separate posting.
- **Oracle Cloud**: 24C added discrete-mfg co/by-product costing. Rollup formula: `cumulative cost = material + resource + overhead − output costs` — clean subtraction for by-product credit.
- **D365**: Process mfg only; formula version associates with co-products + by-products. Discrete BOM-based has no first-class by-product.
- **NetSuite**: Weakest — caller uses journal entries for by-product credit-backs.

**Industry extensions**: Steel mills (slag, scale), oil refining fractionation NRV, pharma residues with regulatory disposal, brewing spent grain, PCB recycling.

**Our gap**: `wo_outputs.allocation_method ∈ {primary, sales_value, fixed_ratio, market_price}` — `market_price` raises P0033. No NRV credit-back; no negative-allocation for disposal-cost.

**Recommendation**:
- Add `wo_outputs.output_role TEXT CHECK (output_role IN ('primary', 'coproduct', 'byproduct_nrv', 'byproduct_negligible', 'byproduct_disposal'))`.
- For NRV: `wo_outputs.nrv_unit_value BIGINT`; in `post_wo_complete`, by-product credited at `qty × nrv_unit_value` to fg, same amount subtracted from primary's drain share (Oracle formula).
- Disposal-cost: post negative-NRV as debit to new `variance_disposal_cost` and credit to `inv_value_wip`.
- Compute `v_total_drain` net-of-NRV before per-output loop in `post_wo_complete`'s pre-balance step (mig 0061's natural insertion point).

## acct-fv1 — Disassembly / reverse manufacturing

**Problem**: Pipeline forward-only. Need disassembly orders for returns, refurbishment, recycling, warranty teardown.

**Industry**:
- **SAP**: PP-DIS / "dismantling production order" — separate order type, separate disassembly BOM (FG positive component, recovered components negative qty). CO11N consumes FG, posts negative-qty back to stock.
- **Oracle Cloud**: Rework + Transform work orders (more flexible than pure disassembly — can swap components selectively). Discrete only.
- **D365**: F&O has weakest standalone story; Business Central sibling has better. F&O typically uses negative-qty formula lines.
- **NetSuite**: First-class **Assembly Unbuild** — single transaction reverses original Build's BOM. Simplest user model.

**Industry extensions**: Right-to-repair (EU, US states); circular economy (Apple, Patagonia); aerospace + medical-device returned-goods inspection.

**Our gap**: No disassembly path. `transfer_reason` enum has no `disassembly_*`. `post_wo_complete` is forward-direction only; `wo_outputs` similarly forward.

**Recommendation**:
- `work_orders.kind TEXT CHECK (kind IN ('build', 'disassembly', 'rework'))`. Disassembly reverses BOM: parent consumed, items become outputs.
- Cost flow: FG drains at FG std/wac avg → distributes across recovered components by NRV-style allocation (caller-supplied `recovery_unit_value` per component, like reverse `wo_outputs`). Residual hits new `variance_disassembly`.
- Reuse `wo_events` table; add `event_kind ∈ {disassembly_start, disassembly_complete}`. Reservation lifecycle gains `'returned'` state.
- NetSuite Unbuild is the closest mental model; SAP's separate-order-type the safer architectural pattern.

## acct-1zd — Alternate routings

**Problem**: `wo_routings` per-WO inline. No alternate-routing structure parallel to BOM2's `alternate_no`.

**Industry**:
- **SAP**: Alternative sequences inside routing + production versions linking BOM × routing pair + alternate resources within an op. Three-axis flexibility.
- **Oracle Cloud**: "Alternate Designator" on routings — paired with alternate-BOM's same-name designator. Constraint-based supply planning evaluates alternates automatically.
- **D365**: Multiple Production Versions as alternative modes; MRP picks by lot size / validity / priority.
- **NetSuite**: WIP & Routings supports multiple routings per assembly; user picks at WO create.

**Our gap**: No `routing_templates` reference table. No alternate-routing concept. `bom_headers.alternate_no` exists; routings don't.

**Recommendation**:
- Mirror BOM2: `routing_headers(id, parent_sku_id, alternate_no, revision_no, code, status, is_primary, effective_at, obsolete_at, eco_id)` + `routing_lines(routing_id, routing_op, op_name, work_center_id, planned_yield_pct, ...)` (merges with acct-c80's per-op-yield).
- `work_orders.routing_id` pins choice at WO create; existing `wo_routings` becomes denormalized snapshot at `post_wo_start` (mirrors `cost_method_at_receipt` snapshot pattern from mig 0087).
- `_wo_resolve_routing_for(p_wo_id, p_business_date)` picks primary at business_date if `routing_id IS NULL`. Multiple primaries → P0033.
- ECO workflow (acct-ir7) extends to routing revisions naturally.

## acct-oi4 — Backflush consumption_policy dispatchers

**Problem**: `skus.consumption_policy` reserves three values; only `forward` is dispatched.

**Industry**:
- **SAP**: Three-level priority hierarchy — routing component-assignment > material-master > work center. CO11N triggers automatic GI on confirmation.
- **Oracle Cloud**: Operation Pull + Assembly Pull supply types — two backflush-scope choices, cleaner.
- **D365**: Flushing principles per BOM line: Start / Finish / Manual / Available on location. "Available on location" is uniquely robust for warehouse-managed environments — flushes actual picked qty.
- **NetSuite**: Completion-time backflush; simplest, least granular.

**Industry extensions**: Repetitive manufacturing (semiconductors, PCB) is almost always backflush-only; continuous-process combines backflush with periodic physical reconciliation.

**Our gap**: `_wo_emit_bom_lines` doesn't branch on `consumption_policy`. P0035 raised for non-`forward`.

**Recommendation**:
- Branch in `_wo_emit_bom_lines` on `parent_sku.consumption_policy`. For `backflush_at_op`, item lines emit ONLY at matching `op_move` (when `applies_at_op == p_to_op`). For `backflush_at_complete`, items defer entirely until `post_wo_complete`.
- Extend `_wo_apply_reason_for` (mig 0047) to check `consumption_policy` and pick firing event window.
- Pricing semantics unchanged across all three policies (literal `qty_per × p_qty × component_std`); only the *event* changes. R5 applies — credit-side SKU drives flagging.
- Defer D365's "Available on location" until we have a picking subsystem.

## acct-ir7 — Full ECO approval workflow

**Problem**: Single-stage `post_eco_approve` with one `approved_by` UUID. No multi-stage routing, no impact analysis, no in-flight WO migration, no supersede logic.

**Industry**:
- **SAP**: ECM (LO-ECH) — change records group multiple object types; built-in workflow with office-inbox routing; formal impact analysis (cost / timeline / quality).
- **Oracle Cloud**: PLM Cloud's seeded ECO workflow Open → Approval → Scheduled → Completed. Multi-stage; rules-based or web-service approval; participant-elapsed-time metrics; redline visualization.
- **D365**: Engineering Change Management ties ECO to product-lifecycle-state transactions (draft / released / obsolete) gating which transactions allow the version. Strong consistency.
- **NetSuite**: SuiteApprovals workflow auto-routes; multi-approver email routing; ECO implements only after approval and creates new BOM revisions automatically.

**Industry extensions**: AS9100 ECN traceability; medical-device CAPA integration; automotive PPAP and tier-1 supplier change notifications. **Effectivity decisions** for in-flight WOs (apply to WIP / new only / cutover date) is the single hardest planner choice.

**Our gap**: No `eco_approval_steps`, no impact analysis (delta cost rollup, WIP value at risk, count of open WOs), no in-flight WO migration, no superseder logic.

**Recommendation**:
- `eco_approval_steps(eco_id, step_no, approver_role, approver_id, status, approved_at, comments)`. `post_eco_approve` becomes `post_eco_approval_step`; final-step transitions ECO to `approved`. Roles drive RBAC (intersects open Q6).
- `eco_impact_report(p_eco_id) RETURNS TABLE`: deltas in std cost rollup per affected sku, count of open WOs by status, projected `inv_value_wip` revaluation amount.
- Effectivity model: add `wo_migration_policy TEXT CHECK (... IN ('apply_to_wip', 'new_wo_only', 'cutover_date'))`. Wire into `post_wo_start` and `_wo_resolve_bom_for`.
- `post_eco_supersede(p_old, p_new)` stamps older as `superseded`, chains `effective_at`.

## acct-8xi — Configurable / super BOMs (variant)

**Problem**: BOM lines reference specific SKU; need class-of-items references resolved at WO create from customer attributes.

**Industry**:
- **SAP**: Variant Configuration (LO-VC) — gold standard. Super BOM + super routing + characteristics + selection conditions + class hierarchy. One super BOM replaces thousands of variant materials.
- **Oracle Cloud**: Oracle Configurator (CZ schema) imports BOM models; supports ATO and PTO; CPQ Cloud BOM Mapping for sales-driven configuration.
- **D365**: Product configuration model with expression and table constraints, written in OML and solved by Microsoft Solver Foundation. Real constraint solver.
- **NetSuite**: Matrix items — parent + sub-items by attribute (Color × Size). 2,000-sub-item cap. Dimensional indexing, not constraint-solver-driven.

**Industry extensions**: Automotive, engineer-to-order machinery, configurable furniture, telecom equipment, configurable PCs/servers.

**Our gap**: No characteristics, no constraints, no class-references-on-bom-lines. `bom_lines.component_sku_id` is a hard FK. `bom_headers.alternate_no` covers pre-defined alternates only.

**Recommendation**:
- Scope to "lite" config-BOM first: `bom_lines.kind = 'configurable_item'` with `component_class_id` (FK to new `sku_classes`) and `selection_rule JSONB`. At WO create, caller passes `configuration JSONB`; `_wo_resolve_configurable_lines(bom_id, config)` returns concrete sku_id per configurable line.
- Hold off on full constraint solver — D365's OML/Solver Foundation is years of engineering. JSONB predicate filters first; upgrade if rule complexity demands.
- Persist resolved configuration on `work_orders.configuration JSONB` at WO create (audit-trail integrity, mirrors `cost_method_at_receipt` snapshot).
- NetSuite matrix-item simplicity is a useful intermediate target for SKU-explosion-avoidance; SAP LO-VC is the long-term north star for engineer-to-order.

---

# Group C — Lot/serial + process manufacturing

All four ERPs separate the **tracking partition** (lot/serial) from the **cost method** so a lot-tracked item can still be averaged or standard-costed. D365's "tracking dimension toggles financial inventory" is the cleanest abstraction: costing dispatches on the same dimensions storage uses.

## acct-uze — Lot/serial tracking (foundation)

**Problem**: No lot_id / serial_id anywhere. `accounts.kind = 'stock_available'` partitioned only by `(sku_id, location_id)`. Cost dispatcher assumes one fungible pool.

**Industry**:
- **SAP**: Batch Management (LO-BM); classification system (class type 022/023) holds chars (`LOBM_VFDAT` shelf life, `LOBM_HSDAT` mfg date); batch determination strategies select FIFO/FEFO/LIFO at GI. Serial profiles (PROFL) gate SN at GR/GI/WM. Batch-specific valuation via `BWTAR`.
- **Oracle Fusion**: Lot control + Serial control as item-master flags. Product Genealogy captures multi-level composition. Specific (lot/serial) costing in actual-cost orgs.
- **D365**: Tracking dimensions (Batch / Serial / Owner / License plate) on dimension group. `Active`, `Physical inventory`, `Financial inventory`, `Coverage plan by dimension` flags control costing partition.
- **NetSuite**: Advanced Inventory Management adds Lot Numbered + Serialized item types. Lot Cost Method (FIFO / FEFO / specific) per item; immutable post-transaction.

**Industry extensions**: FSMA 204 (effective 2026-01) — TLC + KDEs persisted 24 months, FDA delivery in 24 hours; FDA UDI / EU MDR (DI + PI = lot + serial + mfg date); AS9100 §8.5.2 design-life pedigree; chemical CAS / hazmat / lot potency.

**Our gap**: `accounts` no lot/serial partition. `inventory_reservations` no lot_id. Cost dispatcher reads `pool_value/pool_qty` — no lot-scoped pool. No genealogy table.

**Recommendation**:
- Add `lots` + `serials` tables; `lot_id UUID NULL` / `serial_id UUID NULL` columns on `accounts` (CHECK: lot/serial only on `stock_*` kinds); partition uniqueness on `(kind, sku_id, location_id, currency, COALESCE(lot_id), COALESCE(serial_id))`. Existing pools become "lot=NULL" — backward-compatible.
- Reservations gain `lot_id NULL` (NULL = any lot) + `lot_specific` flag.
- `lot_genealogy(parent_lot_id, component_lot_id, qty, work_order_id)` populated by `_wo_emit_bom_lines` and `post_wo_complete` — FSMA-compliant audit table.
- Cost: add `'specific'` and `'lot_fifo'` to `cost_method`. Both compose with R1 per-class qty divisor.

## acct-90x — Multi-tier traceability (genealogy)

**Problem**: FDA UDI / FSMA / aerospace require recursive walks of "this serial built from these lots". Our `transfers` is append-only with `document_id` but no lot linkage.

**Industry**: Convergent — link-by-transaction. SAP MB56 / WLB1 walks parent → component lots; Oracle Trace Genealogy UI tree (forward = end product → components, backward = component → end products); D365 InventTransOrigin recursive walk; NetSuite Inventory Detail subrecord on every transaction.

**Industry extensions**: FSMA 204 (24-month KDE retention, 24-hour FDA delivery); FDA UDI submissions to GUDID, EU UDI to EUDAMED; AS9100 design-life retention (often 30+ years); GS1 Global Traceability Standard (GTIN + batch + serial).

**Our gap**: `wo_outputs` records output_sku + qty but no produced-lot identity. `_wo_emit_bom_lines` issues materials at bom_lines granularity without preserving picked physical lot. No `lot_genealogy` table.

**Recommendation**:
- After acct-uze adds lot_id, materialize `lot_genealogy` view over `transfers WHERE reason IN ('rm_issue_to_wo', 'phantom_explode', 'wo_complete', 'op_move')` joining on `document_id = work_order.id`. Tier-2 mat view per CLAUDE.md tiered read model.
- Forward + backward traversal via recursive CTE on `lot_genealogy`. Depth limit 16 per BOM2 phantom convention.
- Retention: `transfers` append-only (mig 0008) — FSMA's 24-month satisfied. AS9100 design-life retention requires off-volume archival (Phase 3, flag).

## acct-0kz — Catch-weight items

**Problem**: One `qty BIGINT` per transfer. Need parallel UOM (count + weight).

**Industry**:
- **SAP**: Catch Weight Management (CWM) — Base UoM (CASE) + Parallel UoM (KG). Inventory tables carry both; valuation on PUoM. Logistics on BUoM. Integrated with EWM in S/4HANA 1909+.
- **Oracle Fusion**: Dual UOM (Secondary UOM with Pricing UOM / Tracking UOM distinction). Inventory carries both qty values.
- **D365**: Catch weight items (released product type) carry catch-weight unit distinct from inventory unit. PMA add-on integrates with retail POS.
- **NetSuite**: No first-class catch-weight; SuiteApps (NetSuite for Food & Beverage, third-party) add it. Custom field `actual_weight` on lot.

**Industry extensions**: Meat / poultry (USDA-graded weights); cheese (aging weight loss); produce (carton-of-X count varies seasonally); lumber (board-feet vs piece + specific gravity).

**Our gap**: `transfers.qty` single column. `bom_lines.qty_per_parent` single-value. Cost dispatcher reads single-UOM `pool_qty`.

**Recommendation**:
- Add `transfers.qty_secondary BIGINT NULL`, `transfers.uom_secondary TEXT NULL`, `skus.catch_weight_uom TEXT NULL`. NULL on non-catch-weight = backward-compatible.
- Cost dispatcher branch: if `sku.catch_weight_uom IS NOT NULL`, divisor = `qty_secondary` (matches SAP PUoM convention).
- Reservations carry `qty_secondary_estimate` populated at reserve time from per-SKU expected weight; true-up at ship.
- Genealogy from acct-uze composes — each lot of a catch-weight item carries actual weight.

## acct-lle — Process-mfg PI sheets / master recipes

**Problem**: BOM2 is discrete-WO oriented. Need catalysts (consumed-then-returned), recoverable solvents, energy as cost component, batch-size min/max, formula-based ratios, phase DAGs.

**Industry**:
- **SAP**: PP-PI is a separate planning module — Process Order + Master Recipe; phases inside operations with start-finish / finish-start / finish-finish / start-start relationships (DAG, not list). BOM components attached to phases. PI Sheets generated per process order; operators record actual values; data flows back as process messages. Catalysts via backflush + return event; recoverable solvents as co-products with negligible value. **Gold standard.**
- **Oracle**: OPM — Formula + Routing + Recipe + Master Batch Record. Steps allow per-step ingredient consumption. Scaling (linear / fixed / by-charge), min/max batch size, theoretical-vs-actual yield variance.
- **D365**: Formula items + formula lines with `Item type ∈ {Item, Co-product, By-product, Phantom, Planning item}`. Active-ingredient potency with consumption inflation by `(target_potency / actual_potency)`. Batch attributes on lot records.
- **NetSuite**: Advanced Manufacturing covers recipe + formulation; less mature — pharma / chemical often deploy NS + third-party SuiteApp (blendAPPS Formula & Recipe).

**Industry extensions**: Pharma (eBR, 21 CFR Part 11 e-signatures, deviation management); chemicals (hazmat / SDS / reaction mass balance); food (allergen tracking, sanitation gating, FSMA 204 TLC per batch); cement / mining (blending optimization, slurry density, energy as direct material).

**Our gap**: `bom_lines.kind ∈ {item, service, charge}` — no co_product / by_product. `wo_outputs` cost split exists; bom_lines don't link to wo_outputs as inputs/outputs. `wo_routings` flat per-WO — no phase relationships, no DAG. No PI-sheet primitive. No active-ingredient potency. No catalyst return event.

**Recommendation** (phased):
- **Phase 1**: Extend `bom_lines.kind` enum with `'co_product'` + `'by_product'` (additive). Cost split routes through `wo_outputs.allocation_method`.
- **Phase 2**: `wo_phases(wo_id, phase_no, op_id, predecessor_phase_no, relationship_type)` — DAG layer on top of existing `wo_routings`. Existing single-op flat routings get auto-generated single-phase rows for back-compat.
- **Phase 3**: Catalyst + active ingredient gates on acct-uze (need lot attributes for potency). New `transfer_reason` values `catalyst_return`, `solvent_recovery`. Inflation in `_wo_emit_bom_lines` based on `lot_attributes.potency`.
- PI-sheet integration is application-tier; ledger exposes `outbox` event on phase-start / phase-complete.

---

# Group D — AP/AR / financial workflows

The four ERPs converge on the same primitives — they differ mainly in degree of native support vs partner integration.

## acct-3uh — OSP physical custody (outside processing)

**Problem**: Subcontracted ops where parent goods are physically at vendor. Two architectural choices: implicit-via-routing-op vs explicit-vendor-location.

**Industry**:
- **SAP**: 541 (GI to subcontractor's special stock O) / 542 (reverse) / 543 (consume on receipt of finished SCM). Special stock O is a separate inventory class — doesn't pollute unrestricted, stays on our books. **Most robust**.
- **Oracle EBS**: Implicit-custody — WIP requisitions OSP service item; routing op completes via Move transaction backflush. No location move at inventory level.
- **D365**: Both shapes — service item on production BOM (line type "Vendor", flushing principle "Finish") or activity-based subcontracting on route op. Best practice: vendor-managed warehouse with vendor account assigned.
- **NetSuite**: Outsourcing location per vendor (1:1) — explicit physical custody.

**Industry extensions**: Vendor-side cycle-count reconciliation; SOX vendor inventory confirmations at quarter-end; pharma extended-lot traceability through subcontractor; automotive consigned-stock with KANBAN.

**Our gap**: No vendor-side custody concept beyond `vendor_pool` (qty-side accountability for received-but-not-billed, not goods we own at the vendor). No `stock_at_vendor` class.

**Recommendation**:
- Lead with **implicit-custody** for MVP — add `wo_routings.is_outside`, `osp_vendor_id`. Service-line emission in `_wo_emit_bom_lines` mirrors existing `vendor_bill_lines.kind = 'service'`. Reuses GRNI staging without new account_kinds.
- Phase 2 SAP-shape layer: `stock_at_vendor` account_kind (qty-only, partitioned `(sku_id, counterparty_id)`) + reasons `osp_send / osp_consume / osp_return`. State-aware return routing pattern (mig 0086) is the precedent for splitting "still at vendor" vs "consumed".
- Defer NetSuite-style outsourcing-location-per-vendor until cycle-counting matters.

## acct-3dz2 — FX revaluation

**Problem**: We track per-currency-partitioned balances; no home-currency translation, no period-end revaluation.

**Industry**:
- **SAP**: FAGL_FCV (S/4HANA program; FAGL_FC_VAL legacy). Valuates open items + balance-sheet GL accounts in foreign currency; delta vs original posting rate posts to FX gain/loss. **Auto-reverses on first day of next month** (canonical pattern). Material Ledger handles up to three parallel currencies per posting.
- **Oracle Cloud**: Revaluation program adjusts foreign-currency-denominated GL balances at month-end; results post to Unrealized Gain / Unrealized Loss; SFAS 52 / IAS 21 anchored. Revaluation flows to reporting currencies. Auto-reverses next period.
- **D365**: Splits revaluation per sub-ledger (GL / AR / AP / Bank) to avoid double-counting. Realized + unrealized gain/loss accounts per posting profile.
- **NetSuite**: OneWorld's Revalue Open Currency Balances per subsidiary. Foreign Currency Variance Mapping splits by direction or currency. Auto-reverses.

**Industry extensions**: SOX 404 auditor-traceable trail; IAS 21 monetary-vs-non-monetary; ASC 830 functional currency per LE; CTA for consolidation; ASC 815 / IFRS 9 hedge accounting.

**Our gap**: No `home_currency` / `functional_currency` concept (no entity table — single ledger). No `post_fx_revaluation`. No `unrealized_fx_gain` / `unrealized_fx_loss` / `realized_fx_gain` / `realized_fx_loss` account_kinds. `fx_rates` exists but never consulted by `post_transfers` for valuation. No reverse-next-period mechanic.

**Recommendation**:
- Add `home_currency` config + four new `account_kind` values (`unrealized_fx_gain`, `unrealized_fx_loss`, `realized_fx_gain`, `realized_fx_loss`) — currency-only, un-partitioned, P&L. Mirror existing `variance_*` pattern.
- `post_fx_revaluation(p_period_id, p_revalue_at_rate_date, p_actor, p_idempotency_key)` walks open per-counterparty per-currency `ap` / `ap_unsettled` / `ar` / `ar_unsettled` / cash balances; emits paired `unrealized_fx_*` events. Auto-reverse on period+1 day-1.
- Realized FX from `post_ar_payment` / `post_ap_payment` settling at different rate than invoice rate (currently both ignore cross-currency settlement entirely; pre-req before reval).
- Treat WIP / inventory as non-monetary per ASC 830 / IAS 21 — exclude from reval.

## acct-rp2 — Sales tax integration

**Problem**: Caller-supplied `tax_amount`. Three patterns: external engine, internal rules, hybrid.

**Industry**: **All four are hybrid.**
- **SAP**: Native condition-technique (TAXIN, 0TXUSX) + certified Vertex / Avalara / ONESOURCE via SAP CPI / External Sales and Use Tax Calculation framework.
- **Oracle**: E-Business Tax (EBS) / Cloud Tax (Fusion) — native rule-based engine + certified ONESOURCE / Avalara / Vertex integrations.
- **D365**: Internal Tax codes / Tax groups + Tax Calculation Service (TCS) hub for Vertex / Avalara connectors.
- **NetSuite**: SuiteTax (modern engine, monthly rate updates 110+ countries, GST/VAT/withholding/US S&U) + Avalara connector for complex US multi-state.

**Industry extensions**: Post-Wayfair nexus determination; EU VAT MOSS / IOSS; reverse-charge VAT B2B EU; SAF-T compliance; e-invoicing mandates (Italy SDI, India IRP, Mexico CFDI, Brazil NFe).

**Our gap**: No tax-determination concept. No `tax_jurisdictions`, `tax_codes`. No `sales_tax_payable` jurisdiction partitioning. mig 0081 design note locks ship-side tax as "source of truth" so any engine has to slot in BEFORE `post_so_ship`.

**Recommendation**:
- MVP: thin internal rules — `tax_jurisdictions`, `tax_codes(jurisdiction_id, sku_taxability_class, rate, effective_at)`, `compute_tax(p_sku, p_ship_to, p_ship_from, p_amount, p_business_date)` helper. Call from `post_so_ship` to validate caller-supplied tax (strict equality first; tolerance window later — same pattern as mig 0090's `unit_price_tolerance_pct`).
- Plug-in pattern for external engine: SQL contract `compute_tax_external(jsonb_request) RETURNS jsonb_response`; application layer routes to Avalara / Vertex.
- Partition `sales_tax_payable` by `(jurisdiction_id, currency)` once jurisdictions exist. Mig 0086's split-routing precedent shows multi-account works.

## acct-8gn — Customer credit / vendor advance

**Problem**: Returns / refunds after full payment trip credit-normal/debit-normal CHECKs.

**Industry**: **All four reject Pattern A (allow ar/ap negative)**. All use dedicated credit-balance accounts.
- **SAP**: Special G/L indicators — F-29 customer down-payment with indicator "A"; F-48 vendor down-payment. OBYR (vendors) / OBXR (customers) maps special G/L → alternative reconciliation account. **(B) separate accounts.**
- **Oracle**: On-account credit memos + unapplied receipts as distinct concepts. Refunds explicit-event against on-account credit. Hybrid **(B) + (C)**.
- **D365**: Customer prepayment / Vendor prepayment main-account types. Posting profiles route prepayments to dedicated ledger accounts. **Pure (B)**.
- **NetSuite**: Customer Deposits (advance receipts) + Credit Memos (returns) + Customer Refunds (cash-out). **(B) + (C)**.

**Industry extensions**: SOX revenue-recognition controls (deposits not revenue until ASC 606 obligations met); IFRS 15 same; unclaimed-property compliance for unapplied credits aging beyond threshold (US state escheat); construction milestone billings.

**Our gap**: `accounts_check` enforces debit-normal on `ar`, credit-normal on `ap`. No `customer_deposit` / `vendor_advance` / `customer_credit_balance` / `vendor_debit_balance` kinds.

**Recommendation**:
- Add four `account_kind` values: `customer_credit_balance` (credit-normal, `(counterparty_id, currency)`), `vendor_debit_balance` (debit-normal, ditto), `customer_deposit` (credit-normal, advance receipts), `vendor_advance` (debit-normal, prepayments).
- Extend mig 0086's split-routing to mig 0084 / 0085 / 0088: when a return / credit-memo would push `ar` below zero, route over-clearing portion to `customer_credit_balance`. Add fourth destination column to `customer_return_lines`.
- `post_customer_refund(p_credit_balance_acct, p_amount, p_cash_acct, ...)` posts cash CR / `customer_credit_balance` DR. Symmetric `post_vendor_advance_refund`.
- Customer deposits ride new `post_customer_deposit` analogous to `post_ar_payment`; later invoice clears `customer_deposit` against invoice in new `kind = 'deposit_apply'` line shape.

## acct-tt1 — Cross-currency BOMs

**Problem**: `work_orders.currency` single-valued; component in different currency raises P0010.

**Industry**:
- **SAP**: Material Ledger — three parallel currencies per posting (local / group / profit-center). Cost components roll up in all configured currencies at historical rates. **Heavyweight.**
- **Oracle**: Multi-currency receipts + multiple cost books (legal-currency book + parent-company book). Intercompany invoices in "Standard currency".
- **D365**: Per-legal-entity standard cost; cross-LE manufacturing requires intercompany flows. Within one LE, components convert at consumption posting.
- **NetSuite**: OneWorld base currency per subsidiary (immutable). Inter-subsidiary BOMs via intercompany sales/purchase. Within subsidiary, foreign components translate at receipt rate, held in base currency.

**Our gap**: No translation point in `_wo_emit_bom_lines`. No "home currency" or "group currency" concept. No intercompany shapes.

**Recommendation**:
- MVP: translate at component-issue time. In `_wo_emit_bom_lines`, when component pool's currency ≠ `work_orders.currency`, look up `fx_rates` at `business_date`, emit value-leg in WO currency. Cross-currency variance lives in new `variance_fx_translation` kind. **Oracle / D365 / NetSuite path.**
- SAP Material Ledger parity is heavyweight and mid-market shies away — defer indefinitely.
- Cross-entity manufacturing rides intercompany-PO / intercompany-SO once entities exist (universal across all four ERPs).

## acct-063 — Stable expense-account taxonomy

**Problem**: `vendor_bill_lines.kind = 'service'` accepts caller-supplied `expense_account_id` with no `account_kind` constraint.

**Industry**: **Two-level model is universal** — small fixed top-level type, user-extensible categories underneath.
- **SAP**: Financial Statement Versions (FSV) at COA level + Account Groups (control creation, not posting) + Document Type / Posting Key (rules at FI document layer).
- **Oracle**: Account Hierarchy on Natural Account segment with Asset / Liability / Owner's Equity / Revenue / Expense qualifier. Cross-validation rules reject combinations.
- **D365**: Main account type (Balance Sheet / Asset / Liability / Equity / Revenue / Expense / P&L / Total) + Main account category (~70 standard, extensible). Categories tie to default reports.
- **NetSuite**: Fixed Account Types (COGS / Expense / Income / Bank / AR / AP / Equity / etc.). Subaccount-of for hierarchy within type.

**Industry extensions**: SOX expense classification with auditor-traceable mapping; SaaS rule-of-40 OpEx-vs-COGS split; nonprofit functional-expense reporting; construction job-cost coding.

**Our gap**: `post_ap_bill` 'service' validation only checks `ledger_kind='value'` and currency match.

**Recommendation**:
- Add small fixed taxonomy as `account_kind` values: `expense_utilities`, `expense_professional_services`, `expense_rent`, `expense_software`, `expense_travel`, `expense_other`. Mirror `variance_*` pattern.
- Extend `post_ap_bill` 'service' validation: reject `expense_account_id` whose `account_kind` is NOT in `expense_*` allowlist. New error code (P0050 `service_bill_invalid_expense_account`).
- Avoid separate `expense_categories` reference table at MVP — `account_kind` enum extension matches existing pattern; reporting view (`v_operating_expenses_by_category`) becomes one-liner GROUP BY.
- Defer hierarchy / parent-account semantics until reporting structure pressure justifies it.

---

# Group E — Payroll & burden close

## acct-9e2 — Payroll function for actual labor

**Problem**: We absorb labor at standard via `labor_apply` / `labor_applied`. Actual labor never enters the ledger; `expense_account_kind` on `absorption_classes` reserved for this and unused.

**Industry**:
- **SAP**: HCM Payroll → `PCP0` posting run → FI document. **Symbolic account** in `T52EK` maps wage type → real GL → CO. Employee home cost center (Infotype 0001) = CO receiver. CON1/CON2 + KSII reconcile via actual activity rate recompute.
- **Oracle Cloud**: Payroll Cloud's Cost Allocation Key Flexfield walks cost hierarchy (Element Entry → Element → Assignment → Position → Department → Payroll). SLA generates journals.
- **D365**: Production Floor Execution (clock-in/out against production job) → registrations approved → transferred to periodic payroll job. Job-card journal posts labor leg against cost category → resource → production-order WIP.
- **NetSuite**: SuitePeople posts payroll runs in real-time directly to GL (no batch transfer). Labor Cost Allocation SuiteApp redistributes labor by employee rate.

**Industry extensions**: Mid-market integrates ADP / Paylocity / Gusto via journal-import APIs + separate time-clock data (Kronos / UKG / Deputy). **Plex** posts actual labor at clock-out (not at payroll run); **Epicor Kinetic** has standard / actual / average labor with per-WO labor variance built into cost analyzer.

**Our gap**: No `post_payroll` / `post_labor_actual`. No `payroll_runs` / `payroll_lines` / `time_entries` / `employees` / `labor_rates` tables. No `accrued_payroll` / `cash_payroll` / `labor_expense` kinds.

**Recommendation**:
- `employees` ref table + `labor_rates(employee, effective_at, rate, currency)` + `time_entries(employee, wo_id, routing_op, business_date, hours)`.
- `post_labor_actual(time_entry_id)` debits `labor_expense` (per `absorption_classes.expense_account_kind`) and credits `accrued_payroll`. **Capture event-by-event (NetSuite-style)** rather than batch — cleaner with our append-only model.
- `labor_clearing_runs` + `post_payroll_run` closes `accrued_payroll → cash` (or `ap` for contractors) at payroll-run cadence — Oracle SLA pattern; keeps mid-period payroll-frequency != accounting-period boundary clean.
- Per-class staging via `expense_account_kind` distinguishes direct labor / indirect / setup / OSP without enum changes. Leave `oh_applied` without paired `expense_account_kind` (overhead actuals come from many AP bills + utility postings + depreciation, not single payroll).

## acct-oef — Period-close burden netting

**Problem**: Standard burdens absorbed at standard rates; actual labor + OH from AP bills almost never equal absorbed. At close, over/under-absorbed delta needs to route to variance, optionally distributed across COGS + ending-WIP + ending-FG.

**Industry**:
- **SAP**: CKMLCP cockpit walks CO43 (actual OH) → KKAO (WIP) → KKS1 (variance) → CO88 (settlement); cost-center side KSII (actual activity rate) + CON1/CON2 (revaluation at actual prices). KSII drains cost center to zero. KKS1 has 8 named variance buckets. With Material Ledger, revalues end-to-end raw → COGS. **Most powerful.**
- **Oracle Cloud**: Period Close for Manufacturing — usage / efficiency / rate variances per resource / OH class.
- **D365**: Inventory Close = month-end load-bearing step; settles issues to receipts per item method. Production-order ending — Lot-size / Production-price / Production-quantity / substitution variances per cost group.
- **NetSuite**: Single variance leg per class; no automatic COGS+inventory+WIP redistribution. Cost Variance Analysis SuiteApp drills.

**Industry extensions**: Most mid-market shops elect "write off to COGS" (FASB-permissible immaterial-variance shortcut, ≤5% threshold) over textbook prorate — easier audit, faster close. **Plex** does daily mini-closes; rolls absorbed/actual continuously. **Epicor Kinetic** offers standard + actual on same item simultaneously. SAP's "drain cost center to zero" (KSII / CON2) is unique to SAP's activity-allocation model.

**Our gap**: No `burden_close_hook` registered against `close_period`. No `variance_burden_absorbed` / `variance_overhead_absorbed` kinds. `absorption_classes.expense_account_kind` exists but no function reads it for netting. No "actuals" entry path (gates on acct-9e2 for labor; analogous AP-bill-to-OH-pool routing for OH).

**Recommendation**:
- `burden_close_hook(p_period_id, p_force_partial)` registered on `close_period`. Per active `absorption_class` walk: `absorbed = SUM(transfers.amount where credit acct = applied_account_kind, in-period)`, `actual = SUM(transfers.amount where debit acct = expense_account_kind, in-period)`; if `|delta| > threshold` post single-leg variance through new `variance_burden_absorbed` (or per-class `variance_<code>_absorbed`).
- **Default policy: write off to COGS** (mid-market consensus, simplest). Make prorate-across-COGS+FG+WIP a future per-class flag.
- Sequence the new hook BEFORE wac_periodic_close_hook in `close_period` so absorbed/actual is settled while pool snapshots are live. Bypassable via `p_force_provisional`-style flag for missing `expense_account_kind` (e.g., `oh_std` is NULL today). Composes with absorption_classes runtime taxonomy — new burden classes gain netting automatically.

---

# Group F — Infrastructure / ops

## acct-c4p — Pivot post_transfers to pseudo-sync (shape L) when contention emerges

**Problem**: Sync-inline `post_transfers` benched at 547ms p99 vs L's 36ms under 100-writer contention. Deferred pending real-world signal.

**Industry**: **All four separate "doc accepted" from "doc posted to GL" for some doc types** — async transport is mainstream, not exotic.
- **SAP**: Enqueue server (one per system) holds in-memory lock table at SM12; targets 1–5ms enqueue. Update Task V1 (sync-on-commit critical) + V2 (statistical / summary). Doc number range buffering (RZ12 / SNRO) sidesteps serializing sequence locks.
- **Oracle**: Workflow Background Engine on `WF_DEFERRED` AQ; Create Accounting → subledger journal staging → GL_INTERFACE batched.
- **D365**: Batch framework with Subledger transfer to GL in *async* mode is documented escape hatch; `LedgerJournalMultiPost` parallelizes on batch server. **Architectural twin of our shape L.**
- **NetSuite**: SuiteCloud governance enforces concurrency limits per integration / SuiteCloud Plus license. Heavy work goes to Map/Reduce queue (separate pool, doesn't count against API budget).

**Industry extensions**: Odoo runs accounting moves inline + Celery for bulk imports; ERPNext uses `frappe.enqueue` for stock-ledger reposting; iDempiere runs accounting fact generation through server-side scheduler.

**Our gap**: Shape L infrastructure built and benched but not default. One transport, no queue depth signal, no back-pressure routing, no automatic L-vs-F selection. Phase 1 fixtures assume sync semantics.

**Recommendation**:
- **Don't make it global.** Mainstream applies async per *document type*: GL-summary posts go async, AR cash receipts stay sync. Add `DrainConfig` selector keyed off doc_type with sync default.
- Build queue-depth metric + SLO tripwire first; don't pivot blind. SAP's 1–5ms enqueue threshold is the model — define our own observed-contention threshold against `pg_stat_activity` waits + `transfers_provisional` lag.
- Treat as Phase 2 Epic infra, not Phase 1 retrofit; piggy-back on §14 Layer 2 structured workload to drive the gate.

## acct-e8g — Convert transfers to time partitioning + idempotency_keys side table

**Problem**: PG forbids `PARTITION BY RANGE(posted_at)` with `UNIQUE(idempotency_key)` (partition key must participate in unique). Plan: side table `idempotency_keys` + partition transfers by `posted_at`.

**Industry**:
- **SAP**: Universal journal ACDOCA on HANA — range partitioning canonical for time-series fact tables; 100–500M rows/partition target, 2B-row HANA limit. NSE moves cold partitions to disk. SARA archives via `FI_DOCUMNT` archive object — writes ADK file, deletes from live tables. **Uniqueness in BKPF is `(MANDT, BUKRS, BELNR, GJAHR)` — fiscal year IS in the natural key**, so partitioning by GJAHR composes naturally with uniqueness. SAP put the partition discriminator *in* the natural key — direct architectural precedent.
- **Oracle Cloud**: GL_JE_LINES (header GL_JE_HEADERS, batch GL_JE_BATCHES); subledger XLA_AE_HEADERS / XLA_AE_LINES linked via GL_SL_LINK_ID. EBS DBAs typically range-partition largest XLA on `ACCOUNTING_DATE` / `CREATION_DATE`. Archival via "Purge Accounting Events / Subledger Transactions".
- **D365**: GeneralJournalAccountEntry hot table; sizing ~10K lines/JournalBatchNumber, ≤10 lines/voucher. No native partitioning exposed (SQL Server / Azure DBA concern). DMF for export-and-purge.
- **NetSuite**: Multi-tenant Oracle; partitioning opaque. Volume limits at saved-search / SuiteAnalytics layer (~10K rows UI, 1K–5K programmatic).

**Industry extensions**: Odoo `account.move.line` unpartitioned in stock PG; large deployments roll monthly partitioning patches. ERPNext partitions GL Entry at scale via DBA. iDempiere relies on PG declarative + mat views.

**Our gap**: Side table + partition shape committed on paper; not modeled. Partition granularity undecided (month / quarter / year). No archival workflow. `transfers` triggers (`append_only`) need partition-aware re-creation.

**Recommendation**:
- Side table `idempotency_keys(idempotency_key, transfer_id)` with FK; `transfers PARTITION BY RANGE(posted_at)` monthly.
- Bake partition creation into `close_period` (next-period roll creates next partition). Detach + ATTACH for archival once retention policy lands (separate ticket).
- **Don't archive in MVP** — partition for query pruning, not bytes-on-disk. SARA-style file-out is Phase 3 audit-retention concern.

## acct-9ij — Negative inventory support (oversold / catch-up)

**Problem**: `accounts` CHECK enforces non-negative per `normal_side`. Real ERPs ship "permissive" mode for issue-before-receipt lag.

**Industry**: **All four ship strict by default with explicit per-item / per-org / per-plant override.**
- **SAP**: OMJ1 three-level switch — valuation area + plant + material master `Negative Stocks Allowed`. All three must align. MAP recomputes against now-zero/positive balance on catch-up; price-difference posts to variance. Standard price unaffected.
- **Oracle**: Inventory Org parameter Allow Negative Balances. Under average costing, depletion-below-zero creates **Average Cost Variance (ACV)**: qty needed to bring on-hand to zero valued at *current* average; remainder valued at "normal transaction unit cost"; difference → cost variance account. **Cleanest model — quarantines valuation error in dedicated GL line.**
- **D365**: Two switches on Item Model Group — Physical negative inventory + Financial negative inventory. Documented stance: "in general, allow financial negative" (vendor invoices lag receipts). Best practice: set fallback / default cost on every item that might go negative. Inventory Close walks issues chronologically, matches to receipts, posts true-up vouchers.
- **NetSuite**: Allow Negative Inventory company-level + Prevent Negative Inventory per item. **Use Cost Estimate for Negative Inventory** chooses Last Purchase Price | Zero | Average Cost. Applies to FIFO / LIFO / Specific / Lot only — Average handles automatically.

**Industry extensions**: Odoo *defaults to permissive* (silent negative); `stock_no_negative` community module flips strict. ERPNext per-item Allow Negative Stock + revaluation-on-receipt (FIFO catch-up automatic). iDempiere blocks by default; per-Product-Category config.

**Our gap**: CHECK unconditional, no per-account / per-SKU / per-location override flag, no `variance_negative_inventory` kind, no catch-up workflow. wac_perpetual already raises P0006 on empty-pool.

**Recommendation**:
- `accounts.allow_negative` column (default FALSE, mirror of OMJ1 plant flag); rewrite CHECK to `allow_negative OR <existing predicate>`.
- **Adopt Oracle ACV model**: depletion driving wac_perpetual pool below zero values over-issue at configured fallback (last receipt / standard / zero — caller-supplied SKU policy); routes variance through `variance_negative_inventory`.
- Catch-up workflow on next receipt — recomputes MAP, posts true-up. Wire to `post_po_receipt` under solo-at-pool detection (R3).
- Strict mode stays default; permissive is opt-in.

## acct-7h4 — Period reopen workflow

**Problem**: `periods.closed_at` set by `close_period`, never reset. Real ERPs allow reopen with audit, gated by authority, often cascading.

**Industry**: **Three-state model is universal** (Open / Soft-closed / Hard-closed). **Cascade on reopen** is consensus.
- **SAP**: Posting Period Variant (OB52) — authority-checked update. Period intervals (1=normal, 2=special, 3=CO→FI) — third often left open. Financial Closing Cockpit (FCLOCOC) orchestrates with workflow tracking. FAGL_REOPEN family for FI-AA late posts. Audit via Security Audit Log (SM19/SM20).
- **Oracle Cloud**: Three statuses Open / Closed / **Permanently Closed** (terminal). System distinguishes Period Open Event from Period Reopen Event for SLA / reporting subscribers. Cross-module dependency enforced — Receivables refuses reopen if GL closed.
- **D365**: Three statuses Open / **On hold** / Permanently closed. *On hold* = "soft-closed" — no posting allowed, reversibly reopenable. Permanently closed cannot reopen (workarounds heavy).
- **NetSuite**: Reopen checkbox on period record, permission-gated. **Cascading reopen** — reopening period N reopens later closed periods. Allow Non-G/L Changes is separate fine-grained flag (memos / classifications without reopening).

**Industry extensions**: Odoo `account.move.line` `lock_date` per company + stricter `tax_lock_date`; non-financial edits via separate permission. ERPNext locks by Period Closing Voucher; reopen = delete the PCV. iDempiere C_Period.PeriodStatus enum mirrors Oracle.

**Our gap**: `close_period` one-way. No status enum (only `closed_at IS NULL`). No `permanently_closed` terminal. No reopen function, no cascade, no audit on reopen. Per-call `p_override_closed_period` on returns is awkward midpoint.

**Recommendation**:
- Replace `closed_at` with `period_status` enum (`open` / `soft_closed` / `permanently_closed`) + `closed_at` history table; mirror F&O three-state.
- `reopen_period(p_period_id, p_actor, p_reason TEXT)` cascades to all later soft-closed periods, raises if any permanently closed (P0050+), writes audit rows. Separate role / capability from poster.
- Once reopen lands, **retire per-call `p_override_closed_period` flags** on returns — they should call `reopen_period` then `close_period` again, with audit, instead of bypassing silently.

---

# Recommended pickup order

Based on tractability, design dependence, and architectural ordering across the 27 issues:

## Tier 1 — Standalone / autonomous-tractable (good "next-up" picks)

| Issue | Why now |
|---|---|
| **acct-c80** (per-op planned yield) | Self-contained extension to `wo_routings` + variance reporter. Composes cleanly with existing `yield_mode` decision. |
| **acct-7t4** (by-products NRV) | Additive to `wo_outputs`; clear enum extension; fits existing pre-balance step in `post_wo_complete` (mig 0061). |
| **acct-fv1** (disassembly) | New work-order kind; reuses BOM2 / `wo_events` machinery. NetSuite's Unbuild as the model is uncomplicated. |
| **acct-1zd** (alternate routings) | Mirror of BOM2 alternate_no on routings; predictable schema cost. Unlocks acct-c80 + acct-ir7 routing-variance work. |
| **acct-063** (expense taxonomy) | Smallest scope — `account_kind` enum extension + 1-line validator. Improves AP reporting credibility immediately. |
| **acct-cms** (provisional cost sources) | One column + dispatcher branch; extends existing wac_periodic / wac_retroactive hooks. |

## Tier 2 — Larger but well-shaped (need a design conversation but not foundational)

| Issue | Why deferred |
|---|---|
| **acct-3uh** (OSP custody) | Implicit-custody MVP is small but the SAP-shape Phase 2 layer is real architectural work. Pick implicit first. |
| **acct-ir7** (full ECO workflow) | Multi-stage approval depends on RBAC (Q6 still open). Impact-analysis report is its own non-trivial piece. |
| **acct-oi4** (backflush) | Three policies; fairly mechanical but each needs careful R5 audit on credit-side dispatch. |
| **acct-7h4** (period reopen) | Three-state lattice is real schema work; cascade logic touches every closed-period guard we've added since mig 0026. |
| **acct-9ij** (negative inventory) | Oracle ACV is a clean target; per-account flag plus catch-up workflow. Strict-default keeps it safe. |
| **acct-tt1** (cross-currency BOMs) | Translate-at-issue is clean; needs new `variance_fx_translation` kind + careful R2 dispatch. |

## Tier 3 — Big features with substantial design work

| Issue | Why heavy |
|---|---|
| **acct-uze** (lot/serial foundation) | Touches every inventory function; new tables (`lots`, `serials`) + `accounts` partition extension; reservations rework. **Foundation for acct-90x, acct-0kz, acct-lle Phase 3.** |
| **acct-3dz2** (FX revaluation) | Needs home-currency / functional-currency entity model; auto-reverse-next-period mechanic; 4 new account_kinds. **Unblocks acct-vli (deferred).** |
| **acct-rp2** (sales tax) | Hybrid (internal rules + external plug-in); MVP rules-table is moderate; full Avalara/Vertex integration is application-tier. |
| **acct-8gn** (customer credit / vendor advance) | 4 new account_kinds; refund workflows symmetric to payments; deposit application as new line shape on customer_invoices. |
| **acct-obw** (capacity-based costing) | New tables (cost_categories, routing_operations, rates); refactor of all WO postings touching std_amount. **Prereq for acct-ijt and acct-9e2 meaningful integration.** |
| **acct-ijt** (time-phased rates) | Coordinated with acct-obw — standalone implementation pre-acct-obw is meaningless. |
| **acct-9e2** (payroll actual labor) | New `employees`, `labor_rates`, `time_entries`; `post_labor_actual` + `post_payroll_run`. **Prereq for acct-oef burden netting to be testable end-to-end.** |
| **acct-oef** (burden close) | Hooks into close_period orchestrator; needs acct-9e2's actual-labor entry path to validate properly. |
| **acct-8gg** (FIFO / lot costing) | New layer schema; intersects acct-uze (lot tracking). Defer until WIP-on-WAC stabilizes (we have wac_perpetual / periodic / retroactive — should work first). |

## Tier 4 — Process-mfg cluster (gated on lot/serial)

| Issue | Why gated |
|---|---|
| **acct-90x** (multi-tier traceability) | Depends on acct-uze; once lot_id exists, the genealogy mat view is straightforward. |
| **acct-0kz** (catch-weight) | Composable with acct-uze — each lot carries actual weight. Independent if lot tracking absent. |
| **acct-lle** (process-mfg PI sheets) | Phase 1 (kind enum extension) is small; Phase 2 (wo_phases DAG) is real; Phase 3 (catalysts / active ingredient) gates on acct-uze. |
| **acct-8xi** (configurable BOMs / variant) | Lite version (JSONB selection_rule) is tractable; full constraint solver is years. |

## Tier 5 — Infrastructure deferrals (gated on conditions)

| Issue | Trigger |
|---|---|
| **acct-c4p** (pseudo-sync pivot) | Real-world contention measurement showing F-shape p99 > 500ms or single hot account > 1000 posts/sec. |
| **acct-e8g** (transfers partitioning) | §14.1 exploratory baseline shows partitioning is justified by query / archival patterns. |

---

# Cross-cutting themes (recap)

1. **GRNI / sub-ledger separation is universal** — every ERP keeps unsettled balances out of the cleared accounts. Our `ap_unsettled` / `ar_unsettled` / variance shape is on the right architectural track. Customer credit balances, vendor advances, special stock at vendor are natural extensions.

2. **Strict-by-default with explicit per-resource overrides** — never allow runtime bypasses to do the work that policy flags should do. Negative inventory, period reopen, FX revaluation: per-item / per-period / per-org config, not per-call parameters.

3. **Three-state period model + cascade-on-reopen** is consensus. Replace our binary `closed_at` lattice with `open / soft_closed / permanently_closed`.

4. **Hybrid native + partner integration** is the answer for sales tax (always), payroll (often), tax-jurisdictional content (always), and increasingly lot/serial domain rules. Plug-in patterns over either-or.

5. **Async-per-doc-type, not async-globally**. Apply pseudo-sync where contention exists; keep sync where read-after-write semantics matter.

6. **Translate-at-posting + period-end revaluation** beats parallel-currency-per-row for monetary balances. Material-Ledger-style multi-currency is heavyweight and rarely worth it pre-IPO scale.

7. **Class-confusion checklist (R1–R6) maps to ERP-wide invariants** — not just our naming. Carry these forward into every Phase 2 design review.

---

# Source files

- `/tmp/erp_research_group1_costing.md` — acct-8gg, acct-cms, acct-obw, acct-ijt
- `/tmp/erp_research_group2_bom_extensions.md` — acct-c80, acct-7t4, acct-fv1, acct-1zd, acct-oi4, acct-ir7, acct-8xi
- `/tmp/erp_research_group3_lot_process.md` — acct-uze, acct-90x, acct-0kz, acct-lle
- `/tmp/erp_research_group4_ap_ar.md` — acct-3uh, acct-3dz2, acct-rp2, acct-8gn, acct-tt1, acct-063
- `/tmp/erp_research_group5_payroll_burden.md` — acct-9e2, acct-oef
- `/tmp/erp_research_group6_infra.md` — acct-c4p, acct-e8g, acct-9ij, acct-7h4

Each source file has a complete bibliography with vendor documentation links, KBs, and partner / community references; the synthesis above retains the substantive findings while compressing prose.
