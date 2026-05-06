# REVIEW.md — acct-du2 codebase audit

Pattern-grep audit (Phase 1) over `db/migrations/*.up.sql` for the eight anti-patterns AP1–AP8 from `acct-du2`. Latest migration audited: `0070_wac_retroactive_wip` (Phase 1+2); audit refresh `0071–0090` shipped 2026-05-05 (see addendum below — adds AP9). Function inventory: 15 entry points + ~23 helpers, plus 9 new entry points in 0079–0088 (post_so_ship, post_customer_invoice, post_ar_payment, post_ap_payment, post_so_allocate, post_customer_return, post_po_return, post_customer_credit_memo, post_vendor_debit_memo). This document is the deliverable for Phase 1; Phase 2 (structural per-function audit) extends it with the 7-question per-function notes; the addendum at the bottom extends Phase 2 to cover migrations 0071–0090.

## Anti-pattern reference

- **AP1** stock_available used as a per-class qty divisor.
- **AP2** debit-first SKU resolution on flagging or cost_method dispatch paths.
- **AP3** pool read (`SELECT debits_total - credits_total`) without prior `FOR UPDATE` on the same account.
- **AP4** `qty * resolve_standard_cost_at(...)` not gated by a `cost_method = 'standard'` CASE arm.
- **AP5** mutation of a shared pool without solo-occupancy gate.
- **AP6** variance routing through a debit-normal pool that the same path drained to 0.
- **AP7** `inv_value_*` read not filtered by `currency`.
- **AP8** idempotency-replay check that happens only before `FOR UPDATE` on the primary lock target (acct-69p pattern).
- **AP9** *(added 2026-05-05 audit refresh)* document-level audit field (e.g., line table's `unit_cost` / `cost_method` snapshot) computed from a pre-lock pool read while the **ledger** amount is recomputed post-lock by `_post_transfers_compute_amount`'s WAC two-pass; the persisted snapshot drifts from ledger truth under concurrency. Distinct from AP3: AP3 is "ledger amount is wrong"; AP9 is "ledger amount is right but document audit field disagrees with it." Surfaced by acct-5prc (post_so_ship) and acct-quca (post_standard_cost_roll WIP path).

Verdicts: **clean** / **suspicious** / **bug** / **fix-needed**.

## Per-anti-pattern verdict summary

| AP | Verdict | Notes |
|----|---------|-------|
| AP1 | clean | Historical bug was acct-fii (`post_cost_adjustment` mig 0024 used `stock_available` qty as divisor). Fixed in mig 0069 with per-class signed SUM on `transfers.qty`. All other `stock_available` references in current code are account lookups (`WHERE kind = 'stock_available'`) for posting qty legs, not divisors. |
| AP2 | suspicious | `_post_transfers_apply_event` (mig 0067, latest) uses credit-first `COALESCE` for flagging (acct-7py fix). `_post_transfers_compute_amount` (mig 0031, latest) still debit-first. Functionally equivalent today (cost-event reasons all have NULL `sku_id` on one side) but **drift risk**: any future cost reason with two SKUs on opposite legs would silently pick the wrong side. → **fix-needed (P3 consistency)**. |
| AP3 | fix-needed | Two real hits in `post_wo_complete` (mig 0068): the solo-at-last gate read at line 164 and the residual-sweep gate read at line 350 both read `stock_wip` qty without prior `FOR UPDATE` on that account. → **two sub-issues (P2 each)**. All other pool reads across migrations 0021–0070 are paired with a preceding `PERFORM 1 FROM accounts WHERE id = ... FOR UPDATE` on the same row. |
| AP4 | clean | All current call sites of `resolve_standard_cost_at(...)` in cost-leg paths sit inside `WHEN cost_method = 'standard'` branches: `_post_transfers_compute_amount` (mig 0031), `_wo_emit_bom_lines` (mig 0070, after acct-rgb tier-2 fix in mig 0067), `post_op_move` (mig 0070), `post_wo_complete` (mig 0068), `post_scrap` (mig 0038), `post_po_receipt` (mig 0036), `post_inventory_adjustment` (mig 0027). Standard-only callers (`post_standard_cost_roll` mig 0028) trivially gated. |
| AP5 | clean | Multi-document pool sharing only matters for `inv_value_wip(parent_sku, routing_op)` and `stock_wip(parent_sku, routing_op)`. `post_wo_complete` (mig 0068, acct-69e) gates pre-balance and per-op residual sweep on solo-at-pool. `post_op_move` and `post_scrap` are running-avg-correct against shared pools by construction (running avg is the right per-unit cost regardless of WO origin); no gate needed. |
| AP6 | clean | Close hooks (`wac_periodic_close_hook` mig 0067, `wac_retroactive_close_hook` mig 0070) use single-leg variance routing (orig_debit ↔ variance_acct) at leaf depletions and no-transfer-just-record for internal-chain *_v reasons. `post_wo_close_unproduced` (mig 0056) and the residual sweep in `post_wo_complete` (mig 0068) only post variance against pools that hold non-zero residual — by definition not drained. Design-correct per acct-smn / acct-rso. |
| AP7 | clean | Account partition UK (mig 0020): `inv_value_raw` / `inv_value_fg` keyed on `(sku, location, currency)`; `inv_value_wip` on `(sku, routing_op, currency)`. Every account lookup in current functions includes `currency = ...` in the WHERE. The few unfiltered references (e.g., `post_standard_cost_roll` mig 0028:211 checking for ANY open WIP pool to gate the roll) are currency-agnostic by design. |
| AP8 | fix-needed | One concurrency bug + two consistency drifts. Concurrency: `post_osp_ship` / `post_osp_receive` (mig 0057) only pre-check `transfers.idempotency_key`; concurrent same-key callers race to `post_transfers` and raise UNIQUE violation rather than returning idempotently. → **sub-issue (P2)**. Consistency: `post_op_move` (mig 0070) and `post_scrap` (mig 0038) have only a pre-`FOR UPDATE` replay check on `wo_events`; race defense relies on the `INSERT ON CONFLICT DO NOTHING` at the wo_events row. Safe (no side effects between pool read and INSERT) but wasteful under contention and inconsistent with the acct-69p dual-check pattern used by `post_wo_start`, `post_wo_complete`, `post_wo_close_unproduced`. → **sub-issue (P3)**. |

## Function × AP verdict matrix

Function rows ordered by audit priority (highest-risk first per the bug-class lineage). Latest migration shown in parens.

| Function | AP1 | AP2 | AP3 | AP4 | AP5 | AP6 | AP7 | AP8 |
|----------|-----|-----|-----|-----|-----|-----|-----|-----|
| `post_cost_adjustment` (0069) | clean (acct-fii fix) | n/a | clean (FOR UPDATE at 191/192 before 208) | n/a | n/a | n/a | clean | n/a |
| `_post_transfers_apply_event` (0067) | n/a | clean (credit-first, acct-7py) | n/a | n/a | n/a | n/a | n/a | n/a |
| `_post_transfers_compute_amount` (0031) | n/a | suspicious (debit-first COALESCE) | clean (callers under FOR UPDATE) | clean (standard branch) | n/a | n/a | n/a | n/a |
| `post_wo_complete` (0068) | clean | n/a | **fix-needed** (lines 164, 350) | clean | clean (acct-69e gate) | clean | clean | clean (dual-check at 90, 105) |
| `post_op_move` (0070) | clean | n/a | clean (FOR UPDATE at 419 before 420/422) | clean | clean | n/a | clean | suspicious (single check + INSERT ON CONFLICT) |
| `post_scrap` (0038) | clean | n/a | clean (FOR UPDATE at 914 before 916/918) | n/a | clean | n/a | clean | suspicious (single check + INSERT ON CONFLICT) |
| `_wo_emit_bom_lines` (0070) | clean | n/a | clean (FOR UPDATE at 640/679/723 before pool reads) | clean (standard, wac_perpetual, wac_periodic, wac_retroactive branches) | n/a | n/a | clean | n/a |
| `wac_periodic_close_hook` (0067) | n/a | clean | clean (chronological replay, no shared-pool reads outside lock) | clean | clean | clean (single-leg + internal-chain no-post) | clean | n/a |
| `wac_retroactive_close_hook` (0070) | n/a | clean | clean | clean | clean | clean (same single-leg + no-post) | clean | n/a |
| `cost_adjust_retroactive_hook` (0032) | n/a | clean | clean | n/a | n/a | clean | clean | n/a |
| `post_po_receipt` (0036) | clean | clean (PPV inside standard-only path) | clean | clean | n/a | n/a | clean | n/a (idempotency via transfers + ON CONFLICT in post_transfers) |
| `post_ap_bill` (0035) | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a (no idempotency replay needed; po_receipt-line cumulative match) |
| `post_inventory_adjustment` (0027) | n/a | n/a | clean (FOR UPDATE at 194/195 before 197/199) | clean | n/a | n/a | clean | n/a |
| `post_wo_start` (0070) | n/a | n/a | n/a | n/a | n/a | n/a | clean | clean (dual-check, acct-69p) |
| `post_osp_ship` (0057) | n/a | n/a | clean (FOR UPDATE at 107/108 before 109) | n/a | n/a | n/a | n/a | **fix-needed** (single transfers-key check) |
| `post_osp_receive` (0057) | n/a | n/a | clean (FOR UPDATE at 209/210 before 211) | n/a | n/a | n/a | n/a | **fix-needed** (single transfers-key check) |
| `post_wo_close_unproduced` (0056) | n/a | n/a | clean (FOR UPDATE at 121 before 122) | n/a | n/a | clean (residual only) | clean | clean (dual-check at 66, 77) |
| `post_standard_cost_roll` (0028) | n/a | n/a | clean (FOR UPDATE at 260 before 281) | clean (revaluation path) | n/a | n/a | clean | n/a |
| `post_eco_approve` (0058) | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a (status workflow only; uses FOR UPDATE on eco row) |
| `post_cost_adjustment_retroactive` (0032) | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a (queues row; finalization in close hook) |
| `post_transfers` (0033) | n/a | n/a | n/a (orchestrator; pool reads happen in callees under lock) | n/a | n/a | n/a | n/a | n/a (idempotency at transfers UNIQUE key) |
| `_post_transfers_lock_pre_scan` (0033) | n/a | n/a | n/a (sole purpose IS to acquire FOR UPDATE) | n/a | n/a | n/a | n/a | n/a |
| `_post_transfers_lookup_qty_account` (0021) | n/a | n/a | n/a (read-only metadata fn) | n/a | n/a | n/a | n/a | n/a |
| `_wac_close_pool_qty_in` (0064) | n/a | n/a | clean (called from close hook under post-close-period lock) | n/a | n/a | n/a | clean | n/a |
| `resolve_standard_cost_at` (0027) | n/a | n/a | n/a (read-only; standard_costs is append-only) | n/a | n/a | n/a | n/a | n/a |
| `bom_header_at` (0048) | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| `_wo_resolve_bom_for` (0049) | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| `_wo_explode_bom` (0050) | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a (recursion guard, not idempotency) |
| `_wo_apply_reason_for` (0047) | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| `_wo_burden_events_for_op` (0038) | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| `close_period` (0032) | n/a | n/a | clean (FOR UPDATE on periods row at 101) | n/a | n/a | n/a | n/a | n/a (idempotency via periods.closed_at gate) |
| `run_daily_reconciliation` (0016) | n/a | n/a | n/a (read-only alerter) | n/a | n/a | n/a | n/a | n/a |
| `reserve_inventory` (0014) | n/a | n/a | clean (FOR UPDATE at 46 before qty_promisable check) | n/a | n/a | n/a | n/a | n/a |

## Phase 1 sub-issues (Phase 2 may surface more)

Five flagged. To file under `acct-du2` after this Phase 1 review.

### `acct-du2-A` (P2): post_wo_complete solo-at-last gate reads stock_wip without FOR UPDATE
- **Path**: `db/migrations/0068_post_wo_complete_solo_pool_gate.up.sql:164`.
- **Pattern**: AP3.
- **Hazard**: `v_solo_at_last := COALESCE(v_pool_qty_pre, 0) = p_qty;` decides whether to apply pre-balance variance. Between this read and the drain at `PERFORM post_transfers(v_batch, ...)` (line 327), a concurrent `post_op_move` can move qty into or out of the same `stock_wip(parent, last_op)` pool. False-positive solo classification absorbs other WOs' WIP into THIS WO's `variance_wo_close`. Same-shape bug as the original acct-69e.
- **Fix sketch**: take `FOR UPDATE` on `v_qty_from` (and `v_val_from`) before line 164. Re-read after the lock to get the pinned-state qty.
- **Test**: extend `tests/wo_complete_interleaved_pool.rs` with a scenario where T1 enters `post_wo_complete` past the gate read, T2 commits a `post_op_move` adding qty to the same op, T1 then drains; assert variance_wo_close picks up only T1's contribution.

### `acct-du2-B` (P2): post_wo_complete residual-sweep gate reads stock_wip without FOR UPDATE
- **Path**: `db/migrations/0068_post_wo_complete_solo_pool_gate.up.sql:350`.
- **Pattern**: AP3.
- **Hazard**: `SELECT (debits_total - credits_total) INTO v_op_qty FROM accounts WHERE kind = 'stock_wip' ...` in the residual-sweep loop reads pool qty without locking `stock_wip` first; the lock at line 358 covers the inv_value_wip row only. Same race as A. Severity is lower because residual-sweep absorption is already an approximation (per acct-69e design) and the eventual last-WO close will pick up unabsorbed residue.
- **Fix sketch**: add `PERFORM 1 FROM accounts WHERE id = (stock_wip account id) FOR UPDATE` before line 350.
- **Test**: scenario where two WOs are at the same intermediate op and their close-times overlap such that the gate fires under the race; assert residue lands at the last sharing WO's close, not at THIS WO's `variance_wo_close`.

### `acct-du2-C` (P2): post_osp_ship / post_osp_receive idempotency replay race
- **Path**: `db/migrations/0057_bom2_b11_osp_custody.up.sql:55-57` and `:157-159`.
- **Pattern**: AP8 (the acct-69p shape, applied to OSP).
- **Hazard**: pre-check on `transfers.idempotency_key` then `FOR UPDATE` on `work_orders`. A concurrent T1 with the same key can commit a transfer between T2's pre-check and T2's call to `post_transfers` (line 118 / 221). T2's `post_transfers` then INSERTs into `transfers` and trips the UNIQUE violation, raising an error rather than returning idempotently.
- **Fix sketch**: re-check `transfers.idempotency_key` immediately AFTER the `FOR UPDATE` on `work_orders` (line 68 / 170) — the acct-69p pattern from mig 0039.
- **Test**: two concurrent `post_osp_ship` calls with identical idempotency_key; assert both return successfully, exactly one transfer posted.

### `acct-du2-D` (P3): _post_transfers_compute_amount uses debit-first COALESCE
- **Path**: `db/migrations/0031_wac_retroactive.up.sql:85`, `:310`, `:389`, `:422`.
- **Pattern**: AP2 (consistency).
- **Hazard**: `v_cost_sku := COALESCE(p_d_acct.sku_id, p_c_acct.sku_id)` is functionally equivalent to credit-first today because every cost-event reason has at most one SKU-bearing leg. The bug surfaces only if a future reason puts SKUs on both legs — at which point the dispatcher would pick the wrong side. The flagging path was already migrated to credit-first by acct-7py (mig 0067); leaving the dispatcher debit-first is drift risk, not active bug.
- **Fix sketch**: switch to `COALESCE(p_c_acct.sku_id, p_d_acct.sku_id)` and add a comment citing acct-7py.
- **Test**: T5 conformance case A18_credit_first_sku_resolution (planned for Phase 4).

### `acct-du2-E` (P3): post_op_move / post_scrap missing post-FOR-UPDATE replay check
- **Path**: `db/migrations/0070_wac_retroactive_wip.up.sql:316-318` (post_op_move) and `db/migrations/0038_slice_b_wo_functions.up.sql:850-852` (post_scrap).
- **Pattern**: AP8 (consistency, the acct-69p dual-check pattern).
- **Hazard**: only the pre-`FOR UPDATE` replay check exists; race defense relies on `INSERT INTO wo_events ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING id`. Safe — no side effects between pool read and INSERT, and the NULL-RETURNING bail-out at the end is correct — but wasteful under contention (T2 does pool reads and amount computation that get discarded) and inconsistent with `post_wo_start` / `post_wo_complete` / `post_wo_close_unproduced`.
- **Fix sketch**: add a second `SELECT id INTO v_existing_id FROM wo_events WHERE idempotency_key = p_idempotency_key; IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;` immediately after the `SELECT * INTO v_wo FROM work_orders ... FOR UPDATE`.
- **Test**: covered by existing `tests/idempotency.rs` plus a contention micro-benchmark (optional, not blocking).

## Phase 2 — per-function structural audit

Walks each function with the 7-question checklist:
1. **Scope** — single class? single SKU? single document/WO? single period?
2. **Value-pool reads** — scoped to function's domain or aggregating?
3. **Qty-divisor reads** — scoped or cross-class?
4. **cost_method dispatches** — resolves SKU from depletion source (credit side)?
5. **Document/WO-sharing** — sole-occupancy assumed? gated?
6. **Currency / period boundaries** — handled cleanly?
7. **Post-write invariants** — what holds after return?

Order matches the matrix highest-risk-first.

### `post_cost_adjustment` (mig 0069, latest)

1. **Scope**: single (sku, location, currency, **inventory_class**) — class is an explicit parameter (`p_inventory_class ∈ {raw, fg}`); function adjusts only that class's pool.
2. **Value-pool reads**: line 208 reads `v_pool_value` from the class-specific `v_val_acct` (resolved via `format()` on `inv_value_<class>` at line 159–167). Scoped — clean. ✓
3. **Qty-divisor reads**: line 198–206 reads `v_pool_qty` as the per-class signed SUM on `transfers.qty` filtered to `t.qty IS NOT NULL` and `v_val_acct IN (debit, credit)`. **Class-isolated by virtue of the value-account filter** — this is the acct-fii fix. ✓
4. **cost_method dispatches**: line 119–145 — explicit `CASE` on `v_cost_method`. wac_perpetual NULL-pass (works); standard P0011; wac_periodic / wac_retroactive P0006 (deferred). The function is wac_perpetual-only by design.
5. **Document/WO-sharing**: pool is per-(sku, location, class, currency); shared with concurrent `post_inventory_adjustment` and other `post_cost_adjustment` calls. Lock at 191–192 takes both `v_qty_acct` (stock_available) and `v_val_acct` in ascending id order. Comment at 184–188 explicitly notes the lock-order alignment with `post_inventory_adjustment`. ✓
6. **Currency / period**: currency is an explicit parameter and threads into account lookup at 161–167 (value pool keyed on `currency=$3`). Period gate happens inside the eventual `post_transfers` call (line 270); this function does not directly read or write `periods`. ✓
7. **Post-write invariants**: `inventory_cost_adjustments` row inserted; if `v_delta ≠ 0`, exactly one transfer posted with `cost_adjustment` reason routing through `variance_cost_adjustment`. Pool's value balance after = `p_target_unit_cost × pool_qty` (unchanged qty). Per-class isolation: the OTHER class on the same (sku, location) is unaffected.

**Verdict**: clean post-acct-fii. No new flags.

### `_post_transfers_apply_event` (mig 0067, latest)

1. **Scope**: a single transfer event within a `post_transfers` batch. Receives the loaded debit + credit account rows (assumed under FOR UPDATE by the caller's lock-pre-scan).
2. **Value-pool reads**: none — receives `p_d_acct` / `p_c_acct` already loaded; updates balances via UPDATE (lines 488–491). Reading happens only for ledger_kind / currency validation (already on the row).
3. **Qty-divisor reads**: none. The qty-divisor work happens in `_post_transfers_compute_amount`; this function only persists `v_qty_for_row` from the event payload (line 481–486).
4. **cost_method dispatches**: not for amount — receives `p_cost_method` from caller. Used only for FLAGGING into `transfers_provisional` at line 533–537. Resolves `v_cost_sku` **credit-first** (line 528) — the acct-7py fix. ✓
5. **Document/WO-sharing**: account-level locking is the caller's responsibility (`_post_transfers_lock_pre_scan`). This function sees the post-lock state. ✓
6. **Currency / period**: validates `p_d_acct.currency = p_c_acct.currency` for value events (P0003); resolves period from `business_date` and gates on `closed_at` (P0005, with override). ✓
7. **Post-write invariants**: account `debits_total` / `credits_total` updated atomically; one row inserted into `transfers`; for wac_periodic / wac_retroactive depletions of value-leg, a row inserted into `transfers_provisional`. Returns the new transfer id.

**Verdict**: clean. The credit-first SKU resolution at line 528 is the canonical pattern; future entry points should mirror it.

### `_post_transfers_compute_amount` (mig 0031, latest)

1. **Scope**: pricing of one cost-event leg (op_move / scrap / wo_complete / so_ship — canonical reasons only). Receives loaded debit + credit accounts under the caller's FOR UPDATE.
2. **Value-pool reads**: `v_value_balance := p_c_acct.debits_total - p_c_acct.credits_total` for wac_perpetual (line 118), wac_periodic (line 150), wac_retroactive (line 185). Read off the row received from caller — implicitly under lock. ✓
3. **Qty-divisor reads**: per-class signed SUM on `transfers.qty` filtered to `p_c_acct.id IN (debit, credit) AND qty IS NOT NULL` (lines 105, 137, 172). **Per-class** by virtue of filtering on the class-specific value account. ✓
4. **cost_method dispatches**: `CASE p_cost_method` is exhaustive on the enum. SKU resolution happens in caller; this function trusts `p_cost_method`. **However, line 85**: `v_sku := COALESCE(p_d_acct.sku_id, p_c_acct.sku_id)` is debit-first — and is read for the `standard` branch's `resolve_standard_cost_at` lookup at line 95. For canonical reasons today (op_move/scrap/wo_complete/so_ship), all callers either pass a single-SKU side or have NULL-on-debit (cogs/expense). Drift risk only if a future caller emits a value-leg cost event where both legs carry distinct SKU IDs — see **acct-du2.4**. Note: `post_transfers` itself uses **credit-first** in its lock-pre-scan at line 261 but **debit-first** in the apply loop at line 310/422. Inconsistent within one function; same drift class.
5. **Document/WO-sharing**: pool reads happen on the value account that the caller already locked. WIP class explicitly blocked for wac_periodic / wac_retroactive (lines 125–129, 160–164) — only Phase 2 epic acct-p7v lifts the gate; tier-1/2/3 lifts within the WO wrappers, not here.
6. **Currency / period**: not its concern — caller validates ledger / currency before invocation.
7. **Post-write invariants**: returns BIGINT amount; no side effects.

**Verdict**: drift risk on AP2 (acct-du2.4). Else clean.

### `post_wo_complete` (mig 0068 → wrapped by mig 0070's `_wo_emit_bom_lines`-aware flow)

Latest body lives in mig 0068; cost-method dispatch on parent inherits acct-rso (mig 0070) for wac_retroactive lift via the same wac_perpetual / wac_periodic branch.

1. **Scope**: a single WO close (or partial complete with `v_will_close=FALSE`). Mutates this WO's qty/value pools at last_op and FG drain across `wo_outputs`.
2. **Value-pool reads**: line 178 `FOR UPDATE on v_val_from`, then 179 `v_pool_at_last`; line 215 / 216 in standard pre-balance branch. All under lock. ✓
3. **Qty-divisor reads**: line 181 `v_pool_qty` from stock_wip(parent, last_op) — used as wac running-avg divisor for non-standard parents. Read AFTER FOR UPDATE on `v_val_from` (line 178), but the lock targets the inv_value_wip account, **not** the stock_wip account that line 181 reads. Same shape as **acct-du2.1** (gate read at line 164). The FOR UPDATE at line 178 does not protect the qty pool. **Suspicious**: a concurrent post_op_move can change qty between line 181's read and the post_transfers drain at line 327, mis-pricing the unit cost.
4. **cost_method dispatches**: line 168–195 — explicit CASE on `parent_sku.cost_method`. standard → `resolve_standard_cost_at(parent)`; wac_* → source-pool running avg. Resolved on parent SKU (the WO's primary), which is correct per design (parent is depletion source from inv_value_wip @ last_op). ✓
5. **Document/WO-sharing**: gate at line 164–166 (`v_solo_at_last`) decides pre-balance application. Per-op residual sweep at 336–356 has analogous gate at 350–356. Both gates are the acct-69e fix; both have AP3 holes (acct-du2.1, acct-du2.2).
6. **Currency / period**: account lookup includes `currency = v_wo.currency` (line 287). Period gate inherited via `post_transfers`. ✓
7. **Post-write invariants**: variance_wo_close balance reflects ONLY this WO's residual when solo; deferred to last sharing WO when not solo. wo_outputs receive proportional FG drain summing to `v_total_drain`. work_orders.qty_completed advanced.

**Verdict**: AP3 holes confirmed (acct-du2.1, acct-du2.2). **New finding (P3, drift)**: the wac_* branch's `v_pool_qty` divisor read at line 181 has the same lock gap as acct-du2.1 (the lock at 178 covers value, not qty). Filing as **acct-du2.6**.

### `post_op_move` (mig 0070, latest)

1. **Scope**: a single qty move within one WO between two routing_ops on the same parent SKU.
2. **Value-pool reads**: line 419–421 `FOR UPDATE on v_val_from` then read `v_pool_value`. ✓
3. **Qty-divisor reads**: line 422–423 `v_pool_qty` from stock_wip(parent, from_op) — same-shape lock gap as `post_wo_complete`. The FOR UPDATE at 419 covers `v_val_from` (inv_value_wip), not `v_qty_from` (stock_wip). Filing as **acct-du2.7** (P3 drift).
4. **cost_method dispatches**: line 384–437 — CASE on parent.cost_method. standard branch reads BOM via `_wo_explode_bom` and computes per-unit cumulative + per-lot amortized; wac_* uses pool running avg; else P0026.
5. **Document/WO-sharing**: multiple WOs can share `(parent_sku, routing_op)` pools by design. Op-move is running-avg correct against shared pools (the running avg IS the right per-unit cost regardless of WO origin) — no solo gate needed. ✓
6. **Currency / period**: account lookup pinned on `v_wo.currency`. Period inherited. ✓
7. **Post-write invariants**: stock_wip(from_op) qty decreased by p_qty; stock_wip(to_op) increased; inv_value_wip(from_op) value decreased by `v_value_amount`; inv_value_wip(to_op) increased by same plus first-arrival burdens. wo_events row inserted with `event_kind='op_move'`.

**Verdict**: AP8 consistency (acct-du2.5 already filed). **New finding (P3, drift)**: `v_pool_qty` divisor read without FOR UPDATE on stock_wip. Filing as **acct-du2.7**.

### `post_scrap` (mig 0038, latest)

1. **Scope**: scrap p_qty units at one routing_op of one WO.
2. **Value-pool reads**: line 914 `FOR UPDATE on (v_val_from, v_qty_from)` BOTH accounts (via `WHERE id IN (..., ...)`); line 916 reads stock_wip qty, 918 reads inv_value_wip value. **Both under lock.** ✓ — different from `post_op_move` and `post_wo_complete` which lock only the value account.
3. **Qty-divisor reads**: line 916 `v_qty_balance` — under FOR UPDATE thanks to the IN clause at 914. ✓
4. **cost_method dispatches**: NO dispatch on cost_method. Computes `v_unit_cost = v_val_balance / v_qty_balance` regardless of method. For standard: pool's value-per-qty IS std_cum (all WOs at this op put in the same per-qty std). For wac_*: running avg. Both work correctly.
5. **Document/WO-sharing**: pool may be shared across WOs at same op. Running-avg pricing is correct against shared pool (same reasoning as op_move).
6. **Currency / period**: account lookup pins currency. Period inherited. ✓
7. **Post-write invariants**: stock_scrap qty up by p_qty; stock_wip(op) qty down by p_qty; inv_value_wip(op) value down by v_scrap_value; variance_scrap up by v_scrap_value. work_orders.qty_scrapped advanced.

**Verdict**: cleaner than its sibling `post_op_move` — locks BOTH qty and value accounts (line 914). AP8 consistency (acct-du2.5 already filed). No new flags.

### `_wo_emit_bom_lines` (mig 0070, latest)

1. **Scope**: emits batch events for one WO at one routing_op, filtered by p_filter (kind / basis / fire_at / applies_at_op). Per-line dispatch on **component**.cost_method for items, on absorption_class for services/charges.
2. **Value-pool reads**: `v_comp_val_acct` (inv_value_raw of the component) read at line 619 for the wac_* branches; `v_pool_value` at lines 662 / 701 / 745 — all under `FOR UPDATE on v_comp_val_acct` (lines 640 / 679 / 723). ✓
3. **Qty-divisor reads**: `v_pool_qty` per-class signed SUM on `transfers.qty` filtered to `v_comp_val_acct IN (debit, credit)` for wac_perpetual / wac_periodic / wac_retroactive (lines 641–650, 680–689, 724–733). **Per-class** by virtue of value-account filter. ✓
4. **cost_method dispatches**: line 632–761 — explicit `CASE v_comp_cost_method`. Resolved on **component** SKU (line 629), which is the depletion source (credit side of `rm_issue_to_wo` value-leg = inv_value_raw of component). ✓ acct-rgb fix. wac_periodic / wac_retroactive components additionally gate on parent.cost_method matching (P0026 → acct-7eo for mixed methods).
5. **Document/WO-sharing**: component pool may be shared across WOs / multiple BOM lines on the same component. Running-avg pricing is correct against shared pools.
6. **Currency / period**: `v_val_acct_wip` and `v_comp_val_acct` lookups pin currency to `v_wo.currency` (lines 577, 621). Period inherited via `post_transfers` of the returned batch. ✓
7. **Post-write invariants**: returns a jsonb batch (no direct mutations); caller (`post_wo_start` / `post_op_move`) merges into its own batch and posts via `post_transfers`. Internally idempotent: gen_random_uuid for sub-event keys (each line emits 2 transfers max with fresh UUIDs).

**Verdict**: clean. The component-side dispatch is the canonical correct shape — credit-side SKU drives cost_method, FOR UPDATE on the value account before per-class qty SUM and pool-value read.

### `wac_periodic_close_hook` (mig 0067, latest)

1. **Scope**: one period × all wac_periodic-flagged provisional rows in that period. Topological per-pool walk; per-pool single recompute (not per-event).
2. **Value-pool reads**: line 676 `SELECT SUM(t.amount + COALESCE(p.variance_amount, 0))` over debit transfers to the pool, with LEFT JOIN to `transfers_provisional` for the upstream-variance cache. Read for value-in calc, not directly off accounts. **Implicit via transfer log** — append-only safety. ✓
3. **Qty-divisor reads**: `v_pool_qty_in := _wac_close_pool_qty_in(...)` (line 687) — dispatches on pool.kind. For inv_value_wip pools, reads paired stock_wip qty signed sum; for raw/fg, per-class SUM on `transfers.qty`. Helper handles both. ✓
4. **cost_method dispatches**: only processes rows where `cost_method = 'wac_periodic'`. The hook itself doesn't dispatch — it's the receiving end of dispatch decisions made at flag-time.
5. **Document/WO-sharing**: pool-level recompute spans all flagged depletions on that pool, regardless of source document. By design — periodic averaging is cross-document. ✓
6. **Currency / period**: scoped to one period via `p_period_id`. Currency carried on the pool account; variance posts in same currency. ✓
7. **Post-write invariants**: every flagged row in period has `finalized_at NOT NULL` and `variance_amount` set. For leaf depletions: `variance_transfer_id` set; one variance posted single-leg (inv_value_wip pools) or 2-leg wash (raw/fg pools). For internal-chain (op_move_v / rm_issue_to_wo): no transfer, variance recorded for downstream cache. Cycles raise P0036. Empty-receipts pool raises P0020 unless `p_force_provisional=TRUE`.

**Verdict**: clean. The internal-chain no-transfer pattern is the acct-smn fix; tier 2 acct-7py extension to `rm_issue_to_wo` is correct.

### `wac_retroactive_close_hook` (mig 0070, latest)

1. **Scope**: one period × all wac_retroactive-flagged provisional rows. Topological per-pool walk + per-event chronological replay within each pool (acct-rso tier 3).
2. **Value-pool reads**: builds a merged value/qty event stream sorted by `(business_date, doc_chrono, document_id, sub_priority, id)`; for inflows, uses `t.amount + COALESCE(p.variance_amount, 0)` filtered to `variance_transfer_id IS NULL` for the upstream-variance cache. ✓
3. **Qty-divisor reads**: at outflow events, divides running pool_value by running pool_qty maintained through the chronological replay. For inv_value_wip pools, qty events come from paired stock_wip; for raw/fg, qty comes from each row's `t.qty`.
4. **cost_method dispatches**: only `cost_method = 'wac_retroactive'` rows; hook is dispatch destination.
5. **Document/WO-sharing**: pool replay is cross-document. Topological order ensures upstream variance cache resolves before downstream uses it. Cycles raise P0036.
6. **Currency / period**: scoped to one period; currency carried on pool account. Pre-period state computed from transfers `business_date < period_opens` signed SUM (inv_value_wip via stock_wip pair, raw/fg via per-class SUM on value account itself).
7. **Post-write invariants**: every flagged row finalized; leaf depletions get single-leg variance (inv_value_wip source) or 2-leg wash (raw/fg source); internal-chain *_v rows record variance, no transfer. Final pool value should equal `final_avg × pool_qty` after all replays. Mixed parent/component cost methods raise P0026 → acct-7eo.

**Verdict**: clean. The merged value/qty stream + sub_priority ordering (qty inflow → value event → qty outflow) is the acct-rso fix; correctly handles per-event pool_qty pre-decrement.

### `cost_adjust_retroactive_hook` (mig 0032, latest)

1. **Scope**: one period × all un-finalized `inventory_cost_adjustments_retroactive` queue rows targeting that period. For each, walks every credit-side qty-bearing depletion on the pool and posts variance against `variance_cost_adjust_retro`.
2. **Value-pool reads**: doesn't read pool balance — operates on `transfers` table directly (the in-period depletions).
3. **Qty-divisor reads**: per-event `v_prov_unit := v_event.amount / v_event.qty`. The `qty IS NOT NULL` filter at line 288 excludes prior-hook variance transfers (which carry `qty=NULL`). ✓ — naturally idempotent against re-runs of upstream hooks.
4. **cost_method dispatches**: method-agnostic by design (D2). Operator override layered on top of whatever cost was originally applied. wac_periodic / wac_retroactive double-correction is documented and accepted.
5. **Document/WO-sharing**: walks every in-period depletion regardless of source document. By design.
6. **Currency / period**: queue row carries `(sku, location, currency, inventory_class)`. `business_date` validated in-period at queue-time (`post_cost_adjustment_retroactive`). Closed-period-target rejected with P0021 (acct-7h4 reopen workflow).
7. **Post-write invariants**: queue row gets `finalized_at`, `finalized_count`, `total_variance`. Per non-zero-variance depletion: 2-leg wash through `variance_cost_adjust_retro` (line 304–342). WIP class deferred (P0006 → acct-p7v).

**Verdict**: clean. The 2-leg wash is appropriate for raw/fg pools (debit-normal, retain balance); WIP path is gated off until acct-p7v lifts the WIP-class adjustment.

### `post_po_receipt` (mig 0036, latest)

1. **Scope**: one PO receipt × N receipt lines. Each line: one (sku, location, currency, vendor) triple. Class-implicit (always `inv_value_raw` for receipts).
2. **Value-pool reads**: none directly. Resolves `v_val_acct` (inv_value_raw) and `v_ven_val` (ap_unsettled) by lookup; doesn't read pool balances. Cost computation is unit-based: `v_std_cost := resolve_standard_cost_at(...)` for standard or `v_pl.unit_cost` for wac_*.
3. **Qty-divisor reads**: none. Divisor-free pricing.
4. **cost_method dispatches**: line 215–224 — explicit branch on `v_cost_method`. standard → std cost + PPV; wac_* → po unit_cost; fifo/lot raises P0006.
5. **Document/WO-sharing**: receipt line is per-(po, po_line, qty_received). Cumulative-qty check at line 163–171 (P0023 over-receipt) reads `SUM(qty_received) FROM po_receipt_lines WHERE po_line_id = ...` — this is **not** under FOR UPDATE on po_receipt_lines. Two concurrent receipts on the same po_line could both pass the check and both insert, exceeding qty_ordered. **Suspicious** — same shape as the AP8 OSP race but on po_receipt_lines.
6. **Currency / period**: currency on po_line; account lookup pinned. Period inherited via post_transfers.
7. **Post-write invariants**: po_receipt + po_receipt_lines rows; per line: 2 transfers (qty + value) plus optional 3rd (PPV). vendor_pool qty up; ap_unsettled value up by line cost; inv_value_raw up by valued amount.

**Verdict**: clean on AP1–AP7. **New finding (P3 drift)**: cumulative-qty check at line 163 is not under lock on the parent po_line row. Concurrent receipts on the same po_line can race past the over-receipt gate. Filing as **acct-du2.9**.

### `post_ap_bill` (mig 0035, latest)

1. **Scope**: one bill × N bill lines. Each line: po_match (matched to po_line, three-way match) or service (expense). Vendor + currency-scoped.
2. **Value-pool reads**: none directly. Cumulative `qty_billed` + `qty_received` reads from `po_receipt_lines` and `vendor_bill_lines` for matching.
3. **Qty-divisor reads**: none.
4. **cost_method dispatches**: not relevant — bill is post-receipt; cost was set at receipt time.
5. **Document/WO-sharing**: bill_lines reference po_lines. Cumulative-qty check on `vendor_bill_lines` at the matching path reads `SUM(qty)` for prior bills — same race shape as `post_po_receipt`'s qty check. Concurrent bills against same po_line can race past the strict three-way-match gate (P0024).
6. **Currency / period**: currency from bill header; account lookup pinned. Period inherited.
7. **Post-write invariants**: vendor_bills + vendor_bill_lines rows; per po_match line: 2 transfers (ap_unsettled DR / ap CR for valued amount, ap_unsettled clears toward ap). Service lines: 1 transfer (expense_account DR / ap CR).

**Verdict**: clean on AP1–AP7. **Drift candidate** parallel to post_po_receipt: cumulative-qty check is not under lock on po_line. Filing as part of **acct-du2.9** (bundled — same fix shape: lock the po_line row before reading cumulatives).

### `post_inventory_adjustment` (mig 0027, latest)

1. **Scope**: one (sku, location, **inventory_class**, currency) adjustment. Class is explicit parameter `p_inventory_class ∈ {raw, fg}`.
2. **Value-pool reads**: line 347 `v_val_balance` on class-specific `v_val_acct`. Under FOR UPDATE (line 342–343). ✓
3. **Qty-divisor reads**: line 345 `v_qty_balance` on **stock_available** — **CROSS-CLASS qty** divided into per-class value at lines 360 / 381 to compute `v_effective_uc`. **REAL BUG (P1)** — same shape as acct-fii. Filed as **acct-du2.8**.
4. **cost_method dispatches**: line 325–404 — CASE on v_cost_method. standard → resolve_standard_cost_at + reject explicit p_unit_cost (P0011). wac_perpetual → buggy divisor (acct-du2.8). wac_periodic / wac_retroactive deferred (P0006). fifo/lot deferred (acct-8gg).
5. **Document/WO-sharing**: pool-level concurrency same as cost_adjustment. Lock at 342–343 covers both qty + value accounts.
6. **Currency / period**: currency parameter pinned. Period via post_transfers.
7. **Post-write invariants**: inventory_adjustments row; for IN delta: 2 transfers (qty up + value up); OUT: same 2 with sign flip. Pool balance changes by `v_qty_amount * v_effective_uc`. **The wac_perpetual branch's per-class running avg drifts incorrectly** when raw + fg coexist (acct-du2.8).

**Verdict**: P1 finding (acct-du2.8). Same shape as acct-fii in cost_adjustment; never patched in this function.

### `post_wo_start` (mig 0070, latest)

1. **Scope**: one WO start. Loads BOM via `_wo_resolve_bom_for`, validates routing-op coverage, auto-initializes single-output wo_outputs if empty, emits qty wo_start + bom-line batch via `_wo_emit_bom_lines`.
2. **Value-pool reads**: none directly. All cost computation delegated to `_wo_emit_bom_lines` (component-side dispatch).
3. **Qty-divisor reads**: none directly.
4. **cost_method dispatches**: parent gate at line 158 (`v_cost_method NOT IN {standard, wac_perpetual, wac_periodic, wac_retroactive}` → P0026); component-side dispatch happens in `_wo_emit_bom_lines`.
5. **Document/WO-sharing**: wo_events idempotency_key; FOR UPDATE on `work_orders` row at line 140 (the acct-69p dual-check pattern: pre-check at 136, FOR UPDATE at 140, post-check at 146). ✓
6. **Currency / period**: WO carries currency; account lookups pinned. Period via post_transfers.
7. **Post-write invariants**: wo_events row inserted (event_kind='start'); work_orders.status flipped 'draft' → 'released'; qty leg posts qty_target into stock_wip(parent, first_op); BOM emission posts component consumption + first-op burdens.

**Verdict**: clean. acct-69p dual-check pattern correctly applied.

### `post_osp_ship` / `post_osp_receive` (mig 0057, latest)

1. **Scope**: ship qty from stock_wip(parent, op) to stock_consigned_at_vendor (or back). Qty-only — no value movement (value stays in inv_value_wip).
2. **Value-pool reads**: none.
3. **Qty-divisor reads**: balance check at line 109 / 211 reads stock_wip(_consigned) qty under FOR UPDATE on (qty_from, qty_to) at line 107–108 / 209–210. ✓
4. **cost_method dispatches**: none.
5. **Document/WO-sharing**: stock_wip(parent, op) shared across WOs at same op. ship moves THIS WO's qty; running-avg pricing of inv_value_wip is preserved (value stays). Other WOs unaffected.
6. **Currency / period**: WO carries currency (informational, not used here since qty-only). Period via post_transfers.
7. **Post-write invariants**: stock_wip down, stock_consigned_at_vendor up (or reverse for receive). inv_value_wip untouched. Yield-loss surfaces via subsequent post_scrap on the missing units (see comment at line 247–249).

**Verdict**: AP8 already filed (acct-du2.3 — pre-FOR UPDATE replay check only on transfers; no post-FOR UPDATE re-check). No new flags from structural walk.

### `post_wo_close_unproduced` (mig 0056, latest)

1. **Scope**: close a WO with qty_completed=0 AND qty_scrapped=qty_target. Walks ALL inv_value_wip(parent, op_*, ccy) for this WO and absorbs residuals via wo_close_v.
2. **Value-pool reads**: line 122 reads `v_residual` after FOR UPDATE on `v_op_residual.acct_id` at line 121. ✓
3. **Qty-divisor reads**: none — pure value sweep.
4. **cost_method dispatches**: none — variance is residual-driven, not cost-method-driven.
5. **Document/WO-sharing**: **NOT GATED on solo-at-pool**. Walks every inv_value_wip pool this WO's routing touches and absorbs the WHOLE residual into THIS WO's `wo_close_v` against `variance_wo_close`. If another WO shares parent_sku × routing_op and has unfinished WIP, this function's residual sweep absorbs that other WO's WIP into THIS WO's variance. **This is the original acct-69e bug pattern, applied to this function.**
6. **Currency / period**: account lookup pinned on `v_wo.currency`. ✓
7. **Post-write invariants**: wo_events row inserted (event_kind='close_unproduced'). Per-op residual: 1 transfer (wo_close_v reason). work_orders.status → 'closed'. **Variance amount may include other WOs' WIP residue.**

**Verdict**: AP5 finding — solo-at-pool gate missing. Same shape as acct-69e fix that was applied only to `post_wo_complete`. Severity: P2 (less common path than wo_complete; some operators won't hit shared pools, but those who do silently mis-attribute close variance). Filing as **acct-du2.10**.

### `post_standard_cost_roll` (mig 0028, latest)

1. **Scope**: one (sku) standard cost roll. Updates `standard_costs` table; revalues every open inv_value_raw + inv_value_fg pool for that SKU at p_business_date.
2. **Value-pool reads**: line 281 reads `v_pool_qty` from stock_available account; under FOR UPDATE on inv_value_* accounts (line 260) but NOT on stock_available. **AP3 candidate**: stock_available qty drives revaluation delta `v_delta := v_pool_qty * (p_new_cost - v_prior)`. Concurrent inventory-side mutation could skew the delta. However, the revaluation transfer goes through post_transfers which locks all referenced accounts — but stock_available is NOT in that batch (only inv_value_* + variance_std_cost_roll). Read could race against concurrent post_po_receipt / post_inventory_adjustment.
3. **Qty-divisor reads**: same. Note: stock_available qty is **cross-class** here, but revaluation walks inv_value_raw AND inv_value_fg separately and applies the same v_pool_qty to each — which is **WRONG** when raw + fg are at the same location and have different per-class qty distributions. The revaluation should use per-class qty, not cross-class. **AP1 finding (P2)** — same shape as acct-fii but in revaluation context.
4. **cost_method dispatches**: line 149–174 — standard only; raises P0011 for wac_*; P0006 for fifo/lot.
5. **Document/WO-sharing**: skus row FOR UPDATE at line 143 serializes concurrent rolls on the same SKU. ✓ WIP gate at line 209 blocks roll if any open WIP pool has non-zero balance — sidesteps the WIP-revaluation problem (deferred to acct-bru).
6. **Currency / period**: per-currency variance_std_cost_roll lookup inside the loop. ✓
7. **Post-write invariants**: standard_costs row inserted; per pool: 1 transfer (write-up debit pool / credit variance, write-down reverse). v_pool_qty / v_total_delta accumulated in inventory_standard_cost_rolls audit row.

**Verdict**: **NEW P2 findings** —
- (a) AP1: stock_available qty used as per-class revaluation divisor (line 281, applied per inv_value_raw and inv_value_fg pool). Same shape as acct-fii — same-location raw+fg over/under-revalues by the other class's qty share.
- (b) AP3: stock_available read at 281 not under FOR UPDATE; concurrent receipt/adjustment can race the delta computation.

Filing as **acct-du2.11** (a) and **acct-du2.12** (b).

### `post_eco_approve` (mig 0058, latest)

1. **Scope**: one ECO approval. Stamps ECO row, activates attached draft bom_headers, obsoletes prior active for each (parent_sku, alternate_no).
2. **Value-pool reads**: none — pure metadata workflow.
3. **Qty-divisor reads**: none.
4. **cost_method dispatches**: none.
5. **Document/WO-sharing**: ECO row FOR UPDATE at line 39 serializes concurrent approvals. The obsolete-then-activate ordering avoids transient duplicate at `bom_headers_primary` partial UNIQUE index — design correct.
6. **Currency / period**: not relevant.
7. **Post-write invariants**: ECO status='approved' with approved_by/approved_at/effective_at; attached bom_headers status='active' with effective_at; prior active status='obsolete' with obsolete_at. P0031 on bad state.

**Verdict**: clean. Pure workflow function; no class-confusion surface area.

### `post_cost_adjustment_retroactive` (mig 0032, latest)

1. **Scope**: queue a row in `inventory_cost_adjustments_retroactive` for one (target_period, sku, location, currency, inventory_class). No transfers posted at queue time — the close hook flushes.
2. **Value-pool reads**: none — only validates the pool exists (line 172–182 lookup).
3. **Qty-divisor reads**: none.
4. **cost_method dispatches**: method-agnostic by design (D2). WIP class gated off (line 124).
5. **Document/WO-sharing**: no. Queue row is per-target-period/per-class.
6. **Currency / period**: target period FOR-UPDATE-free check at line 140; closed-period rejection P0021 (acct-7h4 reopen workflow). business_date validated in-period.
7. **Post-write invariants**: queue row inserted; idempotent on `idempotency_key`. Closed-target rejected with P0021.

**Verdict**: clean.

### `post_transfers` (mig 0033 body shape; mig 0067 / 0070 add reasons to flagging list)

1. **Scope**: a batch of transfer events in one transaction. Atomic. Orchestrator: pre-scan → lock-pre-scan → per-event apply.
2. **Value-pool reads**: doesn't read pools directly; delegates to `_post_transfers_compute_amount` which reads pre-locked pools.
3. **Qty-divisor reads**: same — divisor work in the dispatcher.
4. **cost_method dispatches**: cost-event reasons {op_move, scrap, wo_complete, so_ship} trigger lock-pre-scan WAC SKU collection. The pre-scan SKU resolution (mig 0031 lines 261–267) is **credit-first** (correct for depletion source). The apply-loop SKU resolution at 310 / 422 is **debit-first** (acct-du2.4) — drift risk; functionally equivalent today.
5. **Document/WO-sharing**: lock-pre-scan acquires FOR UPDATE on every account referenced by the batch in ascending id order — deadlock-safe. ✓
6. **Currency / period**: per-event validation in `_post_transfers_apply_event` (P0001/P0002/P0003/P0004/P0005). ✓
7. **Post-write invariants**: every event in batch produces one transfer row + balance updates; cost-event events with wac_periodic / wac_retroactive cost_method also produce transfers_provisional rows. Returns JSONB array of `{index, result: 'ok' | 'exists'}`.

**Verdict**: AP2 drift (acct-du2.4 already filed). Else clean.

### `_post_transfers_lock_pre_scan` (mig 0033)

Single helper that locks every account referenced by a batch (debit + credit + aux qty ids) in ascending id order. No pool/qty/dispatch/sharing/currency/period concerns. Pure correctness primitive.

**Verdict**: clean.

### `_post_transfers_lookup_qty_account` (mig 0021)

Maps a value-side account (inv_value_raw/fg/wip) → matching qty-side account (stock_available or stock_wip). Read-only metadata.

**Verdict**: clean **with a caveat**. Returns `stock_available` for both `inv_value_raw` AND `inv_value_fg`, since they share the same stock_available pool. **This is the genesis of the per-class qty issue** (acct-fii / acct-du2.8 / acct-du2.11): callers using the returned account's `debits_total - credits_total` as a per-class divisor get cross-class qty. The helper itself is correct; hazard is downstream usage. Recommend a code comment flagging that callers must use per-class signed SUM on `transfers.qty` for divisor work, not the returned account's balance — covered by **acct-du2.13** (filed below) doc-level.

### `_wac_close_pool_qty_in` (mig 0064)

For one value pool × one period, returns `Σ(qty)` of in-period qty inflows. For `inv_value_wip`: reads paired stock_wip account; for raw/fg: per-class signed SUM on the value account itself. Uses debit-side qty SUM (positive inflows only) — Oracle PAC convention. Asymmetric: receipts add, depletions don't subtract. Used by `wac_periodic_close_hook`. Tier 3 (`wac_retroactive_close_hook`) uses a different qty-stream merge approach (acct-rso).

**Verdict**: clean.

### `resolve_standard_cost_at` (mig 0027)

`(sku, business_date) → standard cost`. STABLE — index-backed lookup. Raises P0018 if no standard exists at business_date.

**Verdict**: clean. Single canonical lookup; P0018 gate composes cleanly through `_post_transfers_compute_amount` standard branch and every direct caller.

### `bom_header_at` (mig 0048)

`(parent_sku, alternate_no, business_date) → bom_header`. Enforces uniqueness via `bom_headers_active` partial index; raises P0033 on collision or no-active.

**Verdict**: clean.

### `_wo_resolve_bom_for` (mig 0049)

`(wo_id, business_date) → bom_header`. Honors `work_orders.bom_id` if pinned; otherwise falls back to `bom_header_at(parent_sku, 1, business_date)` for primary alternate.

**Verdict**: clean.

### `_wo_explode_bom` (mig 0050)

Recursively flattens phantom child BOMs into a parent BOM at the parent's `applies_at_op`. 16-level depth cap; cycle detection via depth + path tracking; raises P0032.

**Verdict**: clean. Cycle detection is the recursive-explosion correctness primitive; no class/method/document confusion surface.

### `_wo_apply_reason_for` (mig 0047)

`(absorption_class_id, basis) → transfer_reason`. Maps absorption class + basis to the canonical generic reason (`burden_apply` / `lot_charge_apply`) or pinned reason (`labor_apply` / `oh_apply`). Raises P0026 on unmapped applied_account_kind.

**Verdict**: clean. Pure metadata mapping.

### `_wo_burden_events_for_op` (mig 0038)

Returns burden rows for one (wo, op). Used by older pre-BOM2 paths; superseded by `_wo_emit_bom_lines` for BOM2-era WO lifecycle. Trivial helper.

**Verdict**: clean (legacy; not on hot path post-BOM2).

### `_wo_events_check_consumption_policy` (mig 0060)

BEFORE INSERT trigger on `wo_events` (filtering for `event_kind='start'`). Reads `skus.consumption_policy` of the WO's parent SKU; raises **P0035** if non-`forward`. Backflush dispatchers tracked under `acct-BACKFLUSH` (acct-oi4).

**Verdict**: clean. Single-purpose gate; correct enum handling.

### `close_period` (mig 0032 — body covered inline above)

Already audited as part of `cost_adjust_retroactive_hook` walk. Recap:

1. Scope: one period × all three close hooks.
2. Pool reads: none directly.
3. Qty divisor reads: none.
4. cost_method dispatches: none — orchestrator only.
5. Document/WO-sharing: serializes via `FOR UPDATE` on `periods` row at line 389. Each hook called sequentially.
6. Currency / period: gates on `closed_at` + `force_provisional` + `force_recon`. Two override flags pass through to hooks.
7. Post-write invariants: periods.closed_at + closed_by stamped; un-finalized provisionals raise P0015 unless forced; recon alerts raise P0016 unless forced. Hook return counts aggregated.

**Verdict**: clean. The period-row FOR UPDATE serializes concurrent close attempts (P0014 covers concurrent already-closed). p_actor unvalidated — RBAC tracked as Part VII Q6 (still open, design decision).

### `run_daily_reconciliation` (mig 0016)

Read-only alerter. Walks per-(ledger_kind, currency) double-entry sums; walks reservation over-promise; INSERTs reconciliation_alerts rows. No pool mutations. Called by close_period as the recon gate (and by pg_cron daily).

**Verdict**: clean. Per-ledger double-entry SUM is the B3 fix from Part IV §7 — currency-partitioned, not single-global.

### `reserve_inventory` (mig 0014)

`(sku, location, qty, sales_order, sales_order_line) → inventory_reservation_id | NULL`. Reads `stock_available` qty under `FOR UPDATE` (line 46), checks `qty_promisable ≥ qty`, INSERTs reservation row.

**Verdict**: clean. The FOR UPDATE serializes concurrent reservers; `qty_promisable` is computed inside the transaction's lock window; no race. P0010 if no open stock_available account.

### `fn_block_transfer_modifications` (mig 0008)

BEFORE UPDATE / DELETE trigger on `transfers`. Raises P0008 unconditionally. Implements append-only invariant.

**Verdict**: clean.

### `_bom_line_self_reference_guard` (mig 0044)

CHECK constraint helper preventing a `bom_lines` row whose `component_sku_id` references a SKU that is itself a phantom whose primary BOM directly or indirectly references the parent. Walked at INSERT/UPDATE.

**Verdict**: clean.

## Phase 2 conclusion

Walked 33 functions × 7-question structural checklist. Phase 2 surfaced **8 additional sub-issues** beyond Phase 1's grep pass:

| ID | Severity | Function | AP | Summary |
|----|----------|----------|----|---------|
| acct-du2.1 | P2 | post_wo_complete | AP3 | solo-at-last gate read on stock_wip without FOR UPDATE (Phase 1) |
| acct-du2.2 | P2 | post_wo_complete | AP3 | residual-sweep gate read on stock_wip without FOR UPDATE (Phase 1) |
| acct-du2.3 | P2 | post_osp_ship / post_osp_receive | AP8 | idempotency replay race (acct-69p shape) (Phase 1) |
| acct-du2.4 | P3 | _post_transfers_compute_amount | AP2 | debit-first COALESCE drift risk (Phase 1) |
| acct-du2.5 | P3 | post_op_move / post_scrap | AP8 | dual-replay-check pattern missing (Phase 1) |
| acct-du2.6 | P3 | post_wo_complete | AP3 | wac_* branch reads stock_wip qty without FOR UPDATE (Phase 2) |
| acct-du2.7 | P3 | post_op_move | AP3 | wac_* branch reads stock_wip qty without FOR UPDATE (Phase 2) |
| **acct-du2.8** | **P1** | **post_inventory_adjustment** | **AP1** | **wac_perpetual divisor uses stock_available cross-class qty (acct-fii sibling) (Phase 2)** |
| acct-du2.9 | P3 | post_po_receipt / post_ap_bill | AP3 | cumulative-qty check not under FOR UPDATE on po_line (Phase 2) |
| acct-du2.10 | P2 | post_wo_close_unproduced | AP5 | residual sweep missing solo-at-pool gate (acct-69e shape) (Phase 2) |
| acct-du2.11 | P2 | post_standard_cost_roll | AP1 | revaluation uses stock_available cross-class qty (Phase 2) |
| acct-du2.12 | P3 | post_standard_cost_roll | AP3 | stock_available read not under FOR UPDATE (Phase 2) |
| acct-du2.13 | P3 | _post_transfers_lookup_qty_account | DOCS | function comment should warn callers about per-class divisor hazard (Phase 2) |

**Severity totals**: 1 × P1, 5 × P2, 7 × P3.

**Headline finding**: **acct-du2.8** is the same shape as the original acct-fii (P1) but in a different function. acct-fii fixed `post_cost_adjustment` in mig 0069; `post_inventory_adjustment` was overlooked at the time and never patched. Two sibling P2s (acct-du2.10, acct-du2.11) extend the bug-class to functions that nobody had looked at through the same lens.

**Pattern across all findings**: every cross-class / cross-document / cross-method bug was reachable by asking the 7 questions deliberately. The Phase 1 grep found mechanical anti-patterns; Phase 2's structural walk caught semantic uses (a divisor that's "stock_available qty" passes any grep filter, but the question "is this divisor scoped to my class?" surfaces the bug regardless of how it's spelled in code).

**Phase 2 verdict**: the meta-epic's premise is validated. Pattern-grep alone catches ~38% of class-confusion bugs (5/13 sub-issues from Phase 1); structural walk doubles that. Phase 3 (property-based testing with the 7 invariants) will catch any remaining shapes we haven't yet imagined.

## Sub-issue closure summary

13 of 16 sub-issues closed (12 fixed, 1 false alarm). 3 follow-up property-test binaries remain open as planned Phase 3 deeper work.

| ID | Severity | Status | Closed via |
|----|----------|--------|------------|
| acct-du2.1 | P2 | ✓ closed | mig 0073 (lock-set extension in post_wo_complete) |
| acct-du2.2 | P2 | ✓ closed | mig 0073 (lock-set extension, residual-sweep gate) |
| acct-du2.3 | P2 | ✓ closed | mig 0074 (post_osp_*  dual replay-check) |
| acct-du2.4 | P3 | ✓ closed | mig 0075 (compute_amount credit-first COALESCE) |
| acct-du2.5 | P3 | ✓ closed | mig 0075 (post_op_move + post_scrap dual replay-check) |
| acct-du2.6 | P3 | ✓ closed | mig 0073 (lock-set extension, post_wo_complete wac_*) |
| acct-du2.7 | P3 | ✓ closed | mig 0073 (lock-set extension, post_op_move wac_*) |
| **acct-du2.8** | P1 | ✓ closed | **FALSE ALARM** — Phase 2 audit read older mig 0027 body of post_inventory_adjustment; latest mig 0031 body uses per-class signed SUM correctly (acct-1vr fix landed in mig 0030 before acct-fii) |
| acct-du2.9 | P3 | ✓ closed | mig 0076 (po_line FOR UPDATE before cumulative-qty SUM) |
| acct-du2.10 | P2 | ✓ closed | mig 0072 (solo-at-pool gate for post_wo_close_unproduced) |
| acct-du2.11 | P2 | ✓ closed | mig 0071 (post_standard_cost_roll per-class qty divisor) |
| acct-du2.12 | P3 | ✓ closed | mig 0071 (stock_available read removed → AP3 dissolved) |
| acct-du2.13 | P3 | ✓ closed | mig 0075 (lookup_qty_account COMMENT updated) |
| acct-du2.14 | P3 | ✓ closed | tests/property_wo_lifecycle.rs (random multi-WO interleaving × 4 cost methods) |
| acct-du2.15 | P2 | ✓ closed | tests/property_post_cost_adjustment.rs (multi-class same-location pool isolation) |
| acct-du2.16 | P3 | ✓ closed | tests/property_period_close.rs (close_period × 3 hooks) |

**Final tally**: 1 P1 false-alarm caught at fix-time (re-reading the latest migration body), 5 of 5 P2 bugs fixed, 7 of 7 P3 cleanups fixed, 3 of 3 Phase 3 property-test binaries shipped. 6 fix migrations 0071–0076. ~5 regression tests added plus 3 new property-test binaries (each with 100 random scenarios by default, 200 also clean, via `PROPTEST_CASES`). All test binaries pass after every commit.

**Methodology lesson**: the false alarm on acct-du2.8 surfaced a Phase 2 audit pitfall — when there are multiple `CREATE OR REPLACE FUNCTION` for the same function across migrations, only the LATEST one is the active version. Phase 2's structural walk caught this once (post_cost_adjustment latest = mig 0069) but missed it once (post_inventory_adjustment latest = mig 0031, not 0027). The right verification is `grep -nE 'CREATE OR REPLACE FUNCTION fname' db/migrations/*.up.sql | tail -1` before reading the body. The audit's verdict stays valid — even with one false alarm, 12/13 fixed bugs is a strong ROI on the structural walk.


## Phase 2 — per-function structural audit (continued, migrations 0071–0090)

Extends the original Phase 2 audit (which terminated at mig 0070) with the entry-point functions added or modified in migrations 0077–0090. Reads each function with the same 7-question checklist (Scope / Value-pool reads / Qty-divisor reads / cost_method dispatches / Document/WO-sharing / Currency / Post-write invariants) against AP1–AP8. Section order is most-recent-first (newest migration at the top); the modified entry-points (`post_po_receipt`, `post_ap_bill`, `post_standard_cost_roll`, `_post_transfers_apply_event`, `wac_periodic_close_hook`, `wac_retroactive_close_hook`) follow the new entry-points.

### `post_vendor_debit_memo` (mig 0088, latest)

1. **Scope**: one (vendor, currency) memo × N lines. Each line is either `kind='financial'` (caller-supplied expense GL credit + ap debit) or `kind='goods_return'` (one (sku, location, qty) reversal at caller-supplied unit_cost; no PPV).
2. **Value-pool reads**: none. The function does NOT read any pool balance — it accepts caller-supplied `unit_cost` and computes `amount = qty × unit_cost`. Account lookups (lines 583–589 ap, 676–682 stock_available, 684–691 inv_value_raw, 693–704 vendor_pool) are id-only resolution, not balance reads.
3. **Qty-divisor reads**: none. No averaging, no per-class divisor. Caller-supplied unit_cost.
4. **cost_method dispatches**: none. Standalone debit memo intentionally bypasses cost_method (no original po_line.unit_cost to compare against; PPV not computed by design — documented header note line 41–43).
5. **Document/WO-sharing**: pool may be shared across vendors / WOs; this function only adds value to ap, removes value from inv_value_raw at caller-supplied price. No solo gate needed because no averaging happens.
6. **Currency / period**: currency is an explicit parameter; account lookups pinned (`currency=p_currency` at line 585, 686). `business_date` flows into post_transfers via the batch; `p_override_closed_period` passed through (line 753). Account-currency mismatch on `expense_account_id` rejected at line 633–636.
7. **Post-write invariants**: vendor_debit_memos + vendor_debit_memo_lines rows; financial line: 1 transfer (ap DR / expense CR); goods_return line: 2 transfers (vendor_pool DR / stock_available CR qty leg + ap DR / inv_value_raw CR value leg). No PPV. No transfers_provisional flagging happens because reason is `po_return_to_vendor` (not in the cost-event list).

**Verdict**: clean. Standalone-memo-by-design deliberately bypasses cost dispatch; AP1–AP4 are non-applicable (no pool reads, no divisors). AP5/AP6 non-applicable (pool only credited, never drained-then-revalued). AP7 satisfied (currency-pinned). AP8 satisfied via UNIQUE(idempotency_key) on header table + ON CONFLICT DO NOTHING (line 597) + fast-path replay at lines 566–568.

### `post_customer_credit_memo` (mig 0088, latest)

1. **Scope**: one (customer, currency) memo × N lines. Each line is `kind='financial'` (caller-supplied revenue GL debit + ar credit) or `kind='goods_return'` (one (sku, location, qty, disposition) reversal at caller-supplied unit_cost / unit_price).
2. **Value-pool reads**: none. Mirror of vendor_debit_memo — caller-supplied costs throughout.
3. **Qty-divisor reads**: none.
4. **cost_method dispatches**: none. Same rationale as vendor_debit_memo (no original ship_line to reference; cost_method-aware reversal would require either snapshotted cost_method or running-WAC read — explicitly deferred per header note line 39–43).
5. **Document/WO-sharing**: pool credit-only on inv_value_fg (restock/repair) or stock_scrap/variance_scrap (scrap); no averaging, no solo gate needed.
6. **Currency / period**: currency parameter pinned in account lookups (lines 178, 259, 326, 407, 430). Period flows through post_transfers; `p_override_closed_period` passed through (line 457).
7. **Post-write invariants**: customer_credit_memos + customer_credit_memo_lines rows. financial line: 1–2 transfers (revenue/expense DR / ar CR ± tax). goods_return line: 2–4 transfers (qty leg, value leg, revenue reversal, optional tax). All revenue/tax legs always credit `ar` (cleared) — no state-aware routing per header note line 28–33.

**Verdict**: clean. Same shape as `post_vendor_debit_memo` and same design tradeoffs (PPV not computed, cleared-account-only). AP1–AP7 non-applicable for the same reasons. AP8 satisfied via UNIQUE(idempotency_key) + fast-path replay at line 159–161. Note the goods_return path uses caller-supplied `unit_cost` × `qty` for the cogs reversal value-leg amount; if the customer's actual standing inv_value_fg pool has a different running average (wac SKU), the goods come back into inv_value_fg at a different unit cost than current-pool — by design (audit-trail integrity over WAC accuracy, header line 38–40), but a caller passing wrong unit_cost can drift the pool's per-unit running cost. Caller's responsibility, not this function's.

### `post_po_return` (mig 0089, latest body — supersedes 0085, 0086, 0087)

1. **Scope**: one (vendor) return × N lines. Each line references a `po_receipt_lines` row; per-line state-aware split between staging (ap_unsettled) and cleared (ap) drains.
2. **Value-pool reads**: none directly. Cumulative `qty_received` (line 138–141), `qty_billed` (143–146), prior-return splits (148–156) read on transactional tables — uses `purchase_order_lines` FOR UPDATE at line 136 to serialize concurrent receipts/bills/returns on the same po_line (the acct-du2.9 fix). Pool balances themselves are not read.
3. **Qty-divisor reads**: none. Cost dispatch is unit-based (snapshotted `cost_method_at_receipt` from po_receipt_lines, line 111).
4. **cost_method dispatches**: line 171–179 reads the SNAPSHOTTED cost_method (`v_pl.cost_method_snap`, populated at receipt time per mig 0087). standard → `resolve_standard_cost_at` (P0018-gated); wac_* → po_line.unit_cost; fifo/lot → P0006. The snapshot dispatch SIDESTEPS R2/AP2 entirely because there's no SKU-from-leg coalescing — the cost_method is fixed at receipt time. **Clean by design.**
5. **Document/WO-sharing**: po_line FOR UPDATE at line 136 covers cumulative-state reads. State-aware split at 158–161 computes qty_to_unsettled (drain un-billed first) vs qty_to_ap (drain billed); over-return rejected at line 163–169 (P0047). Mig 0089 adds a third state read: receipt's period closed_at (line 238–242) — this read is NOT under any lock, but `periods.closed_at` is monotonic (only set, never unset; only set by close_period under FOR UPDATE on the period row), so dirty read is safe — race window only narrows the window where a concurrent close transaction has just stamped closed_at; both routes (variance_ppv vs variance_ppv_prior_period_adj) post valid value events with the same total amount, just to different P&L kinds. Defensible.
6. **Currency / period**: currency from po_line carried throughout. Receipt's period lookup at 238–241 keys on `business_date BETWEEN opens_at AND closes_at LIMIT 1` — single period match, AP7-clean. p_override_closed_period (acct-dso) passes through to post_transfers at line 360.
7. **Post-write invariants**: po_returns + po_return_lines rows with split tracking. Per line: 1 qty leg (vendor_pool DR / stock_available CR), 0–1 PPV leg per route (variance_ppv or variance_ppv_prior_period_adj depending on receipt-period closed state), 0–1 inv reversal per route (ap_unsettled or ap DR / inv_value_raw CR). PPV ordering before value preserved (acct-quk insight, line 287 design comment in mig 0085 carries forward). State-aware split sums to qty_returned by CHECK constraint on po_return_lines.

**Verdict**: clean. Mig 0086 already added po_line FOR UPDATE (closes acct-du2.9 for this function). Mig 0087's snapshot dispatch eliminates the AP2 surface that the original mig-0085 body had (which read `skus.cost_method` directly, and would have inverted PPV math under cost-method flips). Mig 0089's prior-period-adj routing is well-formed.

### `post_customer_return` (mig 0086, latest)

1. **Scope**: one (customer) return × N lines. Each line references a `so_shipment_lines` row; per-line state-aware split between staging (ar_unsettled) and cleared (ar) drains for revenue; tax always credits ar_unsettled.
2. **Value-pool reads**: none directly. Cumulative `qty_shipped` (line 598–601), `qty_invoiced` (603–606), prior-return splits (608–616) read on transactional tables; `sales_order_lines` FOR UPDATE at line 595 serializes concurrent ships/invoices/returns on the same so_line (mirror of acct-du2.9). Pool balances NOT read for cost — the function uses snapshotted unit_cost from so_shipment_lines (line 567).
3. **Qty-divisor reads**: none. Tax pro-ration at line 633–637 is `(tax_amount × qty_returned) / qty_shipped` — division within the snapshotted line, NOT a per-class qty divisor. AP1-clean.
4. **cost_method dispatches**: none. cogs reversal value-leg amount is `qty_returned × v_sl.unit_cost` (snapshotted from ship-line, line 775). The mig 0087 column `cost_method_at_ship` exists but is reserved for future use — `post_customer_return` does not currently read it. AP2/AP4 non-applicable.
5. **Document/WO-sharing**: so_line FOR UPDATE at 595 covers cumulative-state reads. State split at 621–622 (drain un-invoiced first); over-return rejected at 624–630 (P0045). Pool inventory routing depends on disposition, but each disposition writes to a distinct (sku, location)-keyed pool — no shared-pool concern.
6. **Currency / period**: currency from ship-line carried throughout. p_override_closed_period passes through to post_transfers at line 843.
7. **Post-write invariants**: customer_returns + customer_return_lines rows with split tracking. Per line: 1 qty leg (per disposition), 1 value leg (cogs reversal — to inv_value_fg for restock/repair, to variance_scrap for scrap), 0–1 revenue reversal per route, 0–1 tax reversal (always to ar_unsettled).

**Verdict**: clean. AP1–AP7 non-applicable / safe. AP8 satisfied via fast-path replay at line 526–528 + UNIQUE(idempotency_key) on header. The cost_method-at-ship snapshot is captured for reserved future use; a follow-up may want to dispatch on it (e.g., wac_perpetual ship → return at recomputed running-avg cogs, not snapshot — out of scope).

### `post_so_allocate` (mig 0083, latest)

1. **Scope**: one SO; flips matching `inventory_reservations` rows from `'active'` to `'allocated'` state. No ledger events, no transfers.
2. **Value-pool reads**: none.
3. **Qty-divisor reads**: none.
4. **cost_method dispatches**: none.
5. **Document/WO-sharing**: pure state transition on rows already keyed by `so_id`. No pool mutation.
6. **Currency / period**: not relevant (no transfers).
7. **Post-write invariants**: so_allocations row inserted; matching reservations transition. Idempotent via UNIQUE(idempotency_key) on so_allocations + ON CONFLICT DO NOTHING (line 85). Re-run after allocate finds 0 active reservations to update — safe.

**Verdict**: clean. Pure workflow function with no pool / cost_method / divisor surface area. AP1–AP8 non-applicable.

### `post_ap_payment` (mig 0082, latest)

1. **Scope**: one (vendor, currency, amount) payment. Single ledger event: `ap(vendor, ccy) DR / cash(ccy) CR`.
2. **Value-pool reads**: none.
3. **Qty-divisor reads**: none.
4. **cost_method dispatches**: none. Reason is `ap_payment` (not in cost-event list).
5. **Document/WO-sharing**: vendor's ap and cash accounts may be shared across many in-flight payments / bills / receipts. No averaging happens; the SUM-style debit/credit balance maintenance is deferred to `_post_transfers_apply_event` under FOR UPDATE.
6. **Currency / period**: currency is an explicit parameter; both accounts pinned (lines 84, 92). Period inherited via post_transfers (line 127). No `p_override_closed_period` — current callers cannot back-post.
7. **Post-write invariants**: ap_payments header row + 1 transfer.

**Verdict**: clean. Mirror of `post_ar_payment` (mig 0081). AP1–AP7 non-applicable. AP8 satisfied via UNIQUE(idempotency_key) + ON CONFLICT (line 105) + fast-path replay at 67–69.

### `post_ar_payment` (mig 0081)

1. **Scope**: one (customer, currency, amount) payment. Single ledger event: `cash(ccy) DR / ar(customer, ccy) CR`.
2. **Value-pool reads**: none.
3. **Qty-divisor reads**: none.
4. **cost_method dispatches**: none. Reason is `ar_payment`.
5. **Document/WO-sharing**: ar pool shared across many invoices / payments / returns; balance maintenance via `_post_transfers_apply_event` under FOR UPDATE.
6. **Currency / period**: pinned account lookups at lines 935–937, 942–944. Period inherited.
7. **Post-write invariants**: ar_payments header row + 1 transfer.

**Verdict**: clean. Mirror of post_ap_payment (one sub-issue's mig later). AP1–AP7 non-applicable. AP8 satisfied via UNIQUE(idempotency_key) at line 957–959.

### `post_customer_invoice` (mig 0090, latest body — supersedes 0081, 0086)

1. **Scope**: one (customer, currency) invoice × N lines. Each line is `so_match` (matched to so_line, three-way tolerance match) or `service` (caller-supplied revenue account). Currency-pinned.
2. **Value-pool reads**: none directly. Reads cumulative `qty_shipped` (line 491–492), `qty_invoiced` (493–495), and `qty_to_ar_unsettled` from prior returns (496–499) — all on transactional tables.
3. **Qty-divisor reads**: none. Tolerance check at 476 divides one snapshotted unit_price by another snapshotted unit_price (percent computation, not a per-class divisor).
4. **cost_method dispatches**: none. Invoice clears ar_unsettled → ar; cost was set at ship time.
5. **Document/WO-sharing**: so_line FOR UPDATE at line 449 (`FOR UPDATE OF sl`) serializes concurrent invoices / shipments / returns on the same so_line — covers the cumulative-qty AP3/AP8 race shape. Same fix shape as acct-du2.9 (already filed in REVIEW.md base, closed by mig 0076 which post-dates the original mig-0081 body).
6. **Currency / period**: currency parameter pinned in all account lookups (411, 513, 547, 636); explicit currency mismatch check on so_line at 462–466. Period inherited via post_transfers.
7. **Post-write invariants**: customer_invoices + customer_invoice_lines rows. Per so_match line: 1 base ar DR / ar_unsettled CR at po-recorded amount + 0/1 tolerance absorption against `variance_match_tolerance`. Per service line: 1 ar DR / revenue CR + 0/1 tax leg (ar DR / sales_tax_payable CR).

**Verdict**: clean. Tolerance-window split (mig 0090) is well-formed: the base leg always uses `v_amount_at_so = v_qty × v_sl.unit_price` (the accrual integrity at ship time, line 529), with a delta absorbed via `variance_match_tolerance`. AP1–AP4, AP7 non-applicable. AP5/AP6 non-applicable (no shared-pool drain). AP3 satisfied via FOR UPDATE on so_line. AP8 satisfied via UNIQUE(idempotency_key) at 423–425.

### `post_so_ship` (mig 0087, latest body — supersedes 0081, 0083)

1. **Scope**: one SO ship × N lines. Each line is one (so_line, qty, [unit_price override], [tax_amount override]). Cost dispatch on FG SKU (ship-side).
2. **Value-pool reads**: line 483–484 reads `v_value_balance` from inv_value_fg(sku, ship_location, ccy). NOT under FOR UPDATE on this account directly — the read is BEFORE the post_transfers call at line 574 which acquires the lock via `_post_transfers_lock_pre_scan`. **AP3 candidate**: between the read at 483 and the lock-pre-scan in post_transfers, a concurrent post_so_ship / post_po_receipt / post_inventory_adjustment can mutate `inv_value_fg` and skew the unit_cost computation. The earlier mig 0081 body (line 482–483) had the same shape; mig 0087 preserves it.
3. **Qty-divisor reads**: line 470–475 computes `v_qty_balance` via per-class signed SUM on `transfers.qty` filtered to `v_val_acct IN (debit_account_id, credit_account_id) AND qty IS NOT NULL` — class-isolated by virtue of the value-account filter. AP1-clean (R1 satisfied; the pattern matches `_post_transfers_compute_amount`'s correct shape and `_wo_emit_bom_lines`'s correct shape). HOWEVER same lock concern as the value-pool read: not under FOR UPDATE on `v_val_acct` at the read site. **AP3 candidate** — read at 470 before any FOR UPDATE.
4. **cost_method dispatches**: line 467–487 — explicit CASE on `v_cost_method` (the SKU's CURRENT cost_method, line 409). standard → resolve_standard_cost_at; wac_* (3-way OR catch implicit at line 469 ELSE branch — actually line 467 IF / 469 ELSE, so wac_* go into the ELSE running-avg branch); fifo/lot rejected upfront at 411–415 (P0006). The dispatch resolves on the FG SKU directly (line 409: `WHERE id = v_sl.sku_id`); NOT credit-first COALESCE — but this is the document-level dispatcher, not the transfer-level one. The post_transfers refactor in mig 0081 will REDISPATCH at apply time using credit-first COALESCE (`COALESCE(v_c_acct.sku_id, v_d_acct.sku_id)` line 145, 205, 247). Both dispatches resolve the same SKU because v_sl.sku_id is also the credit account's sku_id (inv_value_fg(v_sl.sku_id) for the COGS leg). Snapshot at INSERT time at line 504 (`cost_method_at_ship`) populates the column for downstream `post_customer_return` use.
5. **Document/WO-sharing**: stock_available + inv_value_fg pools shared with other shipments / receipts / WO completions on the same (sku, location). No solo gate needed for cost dispatch — running-avg pricing is correct against shared pools (same reasoning as op_move). Reservation flip at 568–572 covers `'active'` AND `'allocated'` (the mig-0083 widening).
6. **Currency / period**: currency from so_line carried; account lookups pinned. Period inherited.
7. **Post-write invariants**: so_shipments + so_shipment_lines rows. Per line: 1 qty leg (customer_pool DR / stock_available CR), 1 cogs leg (cogs DR / inv_value_fg CR), 1 revenue leg (ar_unsettled DR / revenue CR), 0–1 tax leg.

**Verdict**: **suspicious — fix candidate: AP3 lock-gap on cost dispatch**. Lines 470–486 read pool qty/value to compute wac_* unit_cost without FOR UPDATE on the value account at the read site. Same shape as `acct-du2.6` (post_wo_complete), `acct-du2.7` (post_op_move) which the original audit flagged as P3 drift. The downstream post_transfers's lock-pre-scan eventually locks the account, but by then the dispatch has already snapshotted v_unit_cost from a stale read; the snapshot is then PERSISTED on so_shipment_lines.unit_cost at line 503 (becomes the source of truth for `post_customer_return`'s cogs reversal). A concurrent po_receipt landing inventory between line 484 and line 574 makes this WO's COGS quote a per-unit cost that doesn't match the inv_value_fg balance at lock time. Severity P3 because the dispatcher (`_post_transfers_compute_amount`) re-reads under lock and would post different amounts than the snapshot — meaning the LEDGER is correct but the line's `unit_cost` audit field drifts (the actual accounts.balance change at line 488–491 in `_post_transfers_apply_event` uses the dispatcher's recomputed amount, not the document's snapshot — wait, actually look at the line 531 `'amount', v_qty_shipped * v_unit_cost` in the batch, then post_transfers's WAC two-pass replaces it with the locked recompute — so the persisted unit_cost on so_shipment_lines is the pre-lock value but the actual transfer.amount is the post-lock value). Worth filing as P3 audit-trail drift; **same lock-gap pattern as acct-du2.6/.7**.

Also note: mig 0087 line 470–474 uses `WHEN t.debit_account_id = v_val_acct THEN  t.qty WHEN t.credit_account_id = v_val_acct THEN -t.qty END` (no `ELSE 0`); ungrouped CASE returns NULL on no-match, but the WHERE-clause filter `v_val_acct IN (debit, credit)` guarantees a match — so this is functionally equivalent to the shape with explicit `ELSE 0`. Not a bug, just stylistic.

### `_post_transfers_apply_event` (mig 0067, latest body; mig 0081 changed `post_transfers` orchestrator, NOT this function)

The original Phase 2 audit covered this. The relevant delta is: **mig 0081 did not change `_post_transfers_apply_event`** — the SO_ship non-SKU-leg pass-through is implemented in `post_transfers` (the orchestrator) lines 153–159 and 207–213. `_post_transfers_apply_event` itself still uses credit-first COALESCE at line 528 for `transfers_provisional` flagging, exactly as the original Phase 2 entry described.

1. **Scope**: same as Phase 2 entry — single transfer event apply step.
2. **Value-pool reads**: none (caller pre-loaded accounts).
3. **Qty-divisor reads**: none.
4. **cost_method dispatches**: line 522–537 — flags wac_periodic / wac_retroactive depletions for `transfers_provisional`. The flagging list now includes 8 reasons (line 522–524): canonical 4 (op_move / scrap / wo_complete / so_ship), BOM2 *_v reasons (op_move_v / scrap_v / wo_complete_v), and rm_issue_to_wo. SKU resolution at line 528 is **credit-first** (`COALESCE(p_c_acct.sku_id, p_d_acct.sku_id)`). For so_ship's revenue / tax legs (no SKU on either side), `v_cost_sku` is NULL and the IF at 529 is FALSE — no flagging. Correct: those legs are not depletions of inventory pools.
5. **Document/WO-sharing**: caller is responsible for lock pre-scan.
6. **Currency / period**: validated lines 462–478 (P0001 / P0002 / P0003 / P0004 / P0005).
7. **Post-write invariants**: 1 transfer row + balance updates + 0/1 transfers_provisional row.

**Verdict**: clean (unchanged from Phase 2 entry). AP2 R2 satisfied (credit-first COALESCE at line 528 — the canonical correct shape). The mig 0081 orchestrator-level non-SKU pass-through for so_ship value legs does NOT relax flagging in `_post_transfers_apply_event`: NULL SKU on both sides → IF guard at line 529 short-circuits, no flag. **The relaxation is sound — non-SKU value legs (revenue, tax) are not inventory pools and would not be flagged regardless.**

### `wac_periodic_close_hook` (mig 0077, latest body — supersedes 0067)

The Phase 2 entry covered the mig-0067 body. The relevant delta is: **mig 0077 adds a mixed-method branch** at lines 550–629. When walking a wac_periodic-flagged provisional `rm_issue_to_wo` row, the hook now checks if the destination's SKU has a different cost_method (line 554–562); if so, posts SINGLE-LEG variance through `variance_material_mixed` against the component pool (the value pool we are walking), leaving the destination WIP untouched.

1. **Scope**: unchanged — one period × all wac_periodic-flagged provisionals; topological per-pool walk.
2. **Value-pool reads**: unchanged (line 494–503 implicit via transfer log + LEFT JOIN to provisional cache).
3. **Qty-divisor reads**: unchanged (`_wac_close_pool_qty_in` at 505–507).
4. **cost_method dispatches**: NEW mixed-detection at line 554–562 — resolves `v_dest_method` via `accounts a JOIN skus s` on the debit account's `sku_id`. This is debit-side SKU resolution, but it's NOT for cost dispatch — it's for **route detection** (decide which variance kind to use). The depletion source dispatch happens earlier (the row is already wac_periodic-flagged because the credit-side SKU was wac_periodic at flagging time). R2 still satisfied — the wac_periodic recompute still walks credit-side pool. The debit-side read at 558 is to determine "is destination homogeneous wac_periodic, or mixed?" — necessary because internal-chain treatment differs.
5. **Document/WO-sharing**: pool-level recompute spans all flagged depletions. Mixed branch posts single-leg (variance_material_mixed DR/CR ↔ component pool — line 591–615), leaving destination WIP untouched per CLAUDE.md R5 (debit-normal pool that the WO path drained to 0 — single-leg, not 2-leg).
6. **Currency / period**: variance_material_mixed account looked up by currency from the COMPONENT pool (`v_pool_acct.currency`, line 582). AP7-clean.
7. **Post-write invariants**: each flagged row finalized; mixed rm_issue_to_wo → 1 single-leg variance transfer (variance_transfer_id set); homogeneous internal-chain (op_move_v / wac_periodic-destination rm_issue_to_wo) → variance recorded, no transfer; leaf raw/fg → 2-leg wash; leaf inv_value_wip → single-leg.

**Verdict**: clean (mixed-method branch correctly implements R5 single-leg routing). AP1–AP4 unchanged from Phase 2. AP5 satisfied (mixed branch posts only against component pool, never touches destination — which is correct because destination is governed by its OWN cost_method's hook). AP6 satisfied (single-leg routing against credit-normal pool the WO path drains in caller). AP7 satisfied. The only subtle concern is the `accounts a JOIN skus s` lookup at 555–558 has no currency filter, but `accounts.id` is unique so the `WHERE a.id = v_orig.debit_account_id` is sufficient; AP7 not violated.

### `wac_retroactive_close_hook` (mig 0077, latest body — supersedes 0070)

The Phase 2 entry covered the mig-0070 body. The relevant delta is: **mig 0077 adds the analogous mixed-method branch** at lines 1025–1096 of mig 0077. Same shape as wac_periodic's mixed branch, applied to the per-event chronological replay.

1. **Scope**: unchanged — one period × all wac_retroactive-flagged provisionals; topological per-pool walk + per-event chronological replay.
2. **Value-pool reads**: unchanged (merged value/qty stream sorted by (business_date, doc_chrono, document_id, sub_priority, id), with LEFT JOIN to provisional cache for upstream variance).
3. **Qty-divisor reads**: unchanged (running pool_qty maintained through replay).
4. **cost_method dispatches**: line 1025–1035 — resolves `v_dest_method` for mixed detection; same shape as wac_periodic's mig-0077 branch. Cost recompute still uses the credit-side pool's running avg (line 1018–1019: `v_recomputed_avg := v_pool_value / v_pool_qty`; `v_recomputed_amt := v_event.qty * v_recomputed_avg`). R2 satisfied — recompute is on the depletion source pool.
5. **Document/WO-sharing**: pool replay is cross-document. Mixed branch posts single-leg variance_material_mixed against `v_event.credit_account_id` (the component pool we're walking) — line 1066, 1077. Note line 1166, 1188 — homogeneous-wac_retroactive WIP path uses `v_pool_id` for credit/debit on the second wash leg, which is the same as the credit-side pool we're walking; correct.
6. **Currency / period**: variance_material_mixed account looked up by `v_pool_acct.currency` at line 1051. AP7-clean.
7. **Post-write invariants**: each flagged row finalized; per-event chronological pool_qty / pool_value updated; mixed → single-leg variance against component; homogeneous internal-chain → variance recorded, no transfer; leaf inv_value_wip → single-leg, leaf raw/fg → 2-leg wash. Pool_value decrement at lines 1213, 1218 is by `v_recomputed_amt` (provisional row case) or `v_event.orig_amount` (non-provisional case) — chosen correctly per event type.

**Verdict**: clean. Mixed-method branch is correctly implemented. The pre-decrement subtraction logic at lines 1213–1221 is subtle but consistent with the Phase 2 (mig-0070) entry's audit — for inv_value_wip pools the pool_qty is NOT decremented inside the value-event walk (paired with stock_wip qty events via sub_priority ordering), which is what acct-rso requires. AP1–AP7 satisfied.

### `post_po_receipt` (mig 0087, latest body — supersedes 0036)

Original Phase 2 entry covered mig-0036 body. The relevant delta is: **mig 0087 adds `cost_method_at_receipt` snapshot** persisted on po_receipt_lines (line 226–230) and resolves it from the SKU's CURRENT cost_method at receipt-post time (line 163: `SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_pl.sku_id`).

1. **Scope**: unchanged.
2. **Value-pool reads**: unchanged (none; cost is unit-based via standard or po_unit_cost).
3. **Qty-divisor reads**: unchanged (none).
4. **cost_method dispatches**: line 163 reads SKU's current cost_method (single SKU resolution at po_line.sku_id — no COALESCE). For wac_*, value posts at po_unit_cost (no PPV). For standard, PPV computed at lines 209, 264–292. The snapshot at line 229 (`cost_method_at_receipt`) captures this method for downstream `post_po_return` to dispatch on (mig 0087 fix).
5. **Document/WO-sharing**: po_line FOR UPDATE at line 142 (mig 0036-original); cumulative-qty over-receipt check at 153–161 is now under that lock — a cleanup that landed via the acct-du2.9 fix-batch (mig 0076).
6. **Currency / period**: pinned (lines 173, 181–182, 199). Period inherited.
7. **Post-write invariants**: po_receipts + po_receipt_lines rows (now with cost_method_at_receipt). Per line: 1 qty leg (stock_available DR / vendor_pool CR), 1 value leg (inv_value_raw DR / ap_unsettled CR), 0/1 PPV leg.

**Verdict**: clean. Snapshot at INSERT time (line 226–230) is correctly placed AFTER all validation but BEFORE the post_transfers call — meaning the column is committed atomically with the receipt's transfers. The `cost_method` read at line 163 is on the SKU row (no FOR UPDATE), but cost_method changes are infrequent and ALL methods route correctly through the dispatcher gate at 165–169 (fifo/lot rejected). A concurrent SKU `cost_method` flip between line 163 and the post_transfers call at line 295 would cause a one-shot mismatch (snapshot says X, dispatcher uses fresh X' — both consistent with their respective inputs, but the snapshot might be stale). Severity P3 audit-trail drift — same shape as the post_so_ship pre-lock issue noted above; no functional bug because the pricing is unit-based (resolve_standard_cost_at is canonical) not pool-based.

### `post_ap_bill` (mig 0090, latest body — supersedes 0035, 0086)

Original Phase 2 entry covered mig-0035 body. The relevant deltas are: (a) mig 0086 adds po_line FOR UPDATE at line 154 + subtracts `qty_to_ap_unsettled` from prior returns in v_avail at lines 202–206, and (b) mig 0090 adds tolerance-window dispatch.

1. **Scope**: unchanged.
2. **Value-pool reads**: unchanged (none).
3. **Qty-divisor reads**: unchanged (none). Tolerance-pct check at 181 is `ABS(...) * 100.0 / v_pl.unit_cost` — divides snapshotted prices, NOT a per-class qty divisor. AP1-clean.
4. **cost_method dispatches**: not relevant — bill is post-receipt; cost was set at receipt time.
5. **Document/WO-sharing**: po_line FOR UPDATE at 154 (`FOR UPDATE OF pl`) — fix from acct-du2.9. Cumulative `qty_received` (197), `qty_billed` (199), and `qty_to_ap_unsettled` from prior returns (202) all under that lock.
6. **Currency / period**: currency parameter pinned. Period inherited.
7. **Post-write invariants**: vendor_bills + vendor_bill_lines rows. Per po_match line: 1 base ap_unsettled DR / ap CR at po-recorded amount + 0/1 tolerance absorption against `variance_match_tolerance` (mig 0090 — line 251–290). Per service line: 1 expense DR / ap CR.

**Verdict**: clean. Tolerance absorption is well-formed: base leg uses `v_amount_at_po = v_qty * v_pl.unit_cost` (line 234), with delta absorbed via variance_match_tolerance. Out-of-tolerance still raises P0024 (line 182–188). Two micro-concerns:
- The tolerance check at line 181 divides by `v_pl.unit_cost`; if `v_pl.unit_cost = 0` and `v_unit_cost <> 0`, we get division-by-zero. Defensive: line 173 only enters this block when `v_unit_cost <> v_pl.unit_cost`, and a non-zero bill on a zero-cost po_line is a caller bug — but `% 0` would raise SQLSTATE 22012 instead of a clean P0024. Cosmetic; **fix candidate (P4)**: add an explicit `v_pl.unit_cost = 0` arm.
- Tolerance pct comparison uses NUMERIC arithmetic; `v_diff_pct > v_tolerance_pct` at line 182 is fine.

AP1–AP7 satisfied. AP8 satisfied via UNIQUE(idempotency_key) at line 131–133.

### `post_standard_cost_roll` (mig 0078, latest body — supersedes 0028, 0071)

Original Phase 2 entry covered the mig-0028 body. The relevant delta is: **mig 0078 adds `p_revalue_wip` parameter** (default FALSE) that lifts the WIP-present gate and adds a per-pool inv_value_wip revaluation loop (lines 308–382).

1. **Scope**: unchanged for raw/fg path. New WIP path: per inv_value_wip(parent, routing_op, ccy) pool for the SKU, posts `pool_qty × Δstd` against `variance_wip_revaluation`.
2. **Value-pool reads**: existing raw/fg path unchanged. NEW WIP path: line 328–331 reads pool_qty from the PAIRED stock_wip account (the qty side, not the value side) under FOR UPDATE acquired at line 222–224 on `v_lock_ids` which now INCLUDES inv_value_wip values (per the conditional at line 207–219: when p_revalue_wip is TRUE, lock-set adds inv_value_wip). **The lock targets the VALUE pools, NOT the paired stock_wip qty accounts** — line 318–322 finds stock_wip via `s.kind = 'stock_wip' AND s.sku_id = v.sku_id AND s.routing_op = v.routing_op` and reads stock_wip's `debits_total - credits_total` at line 328–331 without locking that account. **AP3 candidate**: same shape as acct-du2.6 / .7 (FOR UPDATE on the value pool does not cover the qty pool). A concurrent post_op_move / post_wo_complete / post_scrap on this `(sku, routing_op)` could change qty between the read at 328 and the post_transfers commit at 408.
3. **Qty-divisor reads**: line 328–331 reads `pool_qty` from stock_wip account `debits_total - credits_total` directly (NOT a per-class signed SUM on `transfers.qty`). For inv_value_wip, this is correct because `stock_wip` is per-(sku, routing_op) and its balance IS parent-qty-in-WIP. The mig 0078 design comment (line 28–34) explicitly justifies why per-class signed SUM on `transfers.qty` won't work for inv_value_wip (mixed component-qty / parent-qty in transfers.qty). **R1 satisfied**: stock_wip is single-class by partition (`sku_id, routing_op` only — location is NOT part of the WIP key).
4. **cost_method dispatches**: line 117–142 — standard-only, P0011 for wac_*, P0006 for fifo/lot. Trivially gated.
5. **Document/WO-sharing**: stock_wip pool may be shared across multiple WOs at the same (sku, routing_op). The revaluation walks each pool ONCE (per its accounts.id) and posts one variance per pool. The WHERE-clause-with-FOR-UPDATE on skus row at line 112 serializes concurrent rolls on the same SKU. But across-WO concurrent transfers on stock_wip are NOT serialized — see (2). The mig 0078 design rests on the "every roll revalues WIP atomically" invariant; the same-transaction post_transfers handles atomicity, but a concurrent op_move during the read window could push qty up or down, causing the delta computation to use a stale qty.
6. **Currency / period**: per-pool currency pinned (line 264 raw/fg, 349 wip). AP7-clean.
7. **Post-write invariants**: standard_costs row inserted at line 194–199; raw/fg variance per pool (mig 0071 shape); WIP variance per pool against `variance_wip_revaluation` (new). Pool's value balance changes by `pool_qty × Δstd`; absorbed labor/OH untouched. Atomic with the standard write per design.

**Verdict**: **suspicious — fix candidate: AP3 lock-gap on stock_wip qty read in WIP revaluation loop**. Lines 328–331 read the paired stock_wip account's `debits_total - credits_total` to drive `v_pool_qty`, but the FOR UPDATE batch at line 222–224 locks only inv_value_wip (the value pool), not stock_wip (the qty pool). Same shape as acct-du2.6 / .7 / .12 (closed via mig 0073 / 0071). The WAC tier's `_wac_close_pool_qty_in` (mig 0064) handles the analogous case differently — it reads the stock_wip balance under the close_period lock context. Here we have no period lock; only the SKU-level FOR UPDATE serializes concurrent rolls. A concurrent post_op_move arriving units to `stock_wip(parent, op)` between the read at 328 and the post_transfers commit at 408 makes `v_pool_qty` stale. Severity: race window narrow but real; magnitude is `Δqty × Δstd` per pool. **File as new sub-issue (P3)**: add `PERFORM 1 FROM accounts WHERE id = v_wip_record.qty_acct FOR UPDATE` immediately after the JOIN-resolution at line 313–326 inside the loop (or extend the lock-set at 207–219 to include stock_wip accounts via a UNION to the existing array build).

The existing acct-du2.11 / .12 closure for raw/fg path used per-class signed SUM on `transfers.qty` (which doesn't need stock_available locking because the read targets transfers, not accounts). The WIP path can't use that approach (per the mig 0078 design note). So the fix is structural: add stock_wip account ids to v_lock_ids when p_revalue_wip is TRUE.

AP1, AP2, AP4 satisfied. AP5 satisfied (the SKU-level FOR UPDATE serializes rolls; concurrent op_move/wo_complete don't write to standard_costs). AP6 non-applicable (variance routes against debit-normal pool that GROWS on positive Δstd; the variance account absorbs the diff cleanly). AP7 satisfied. AP8 satisfied (idempotency_key on standard_costs / inventory_standard_cost_rolls; fast-path replay at 99–104).

---

## Per-anti-pattern verdict summary (15 functions in this addendum)

| AP | Verdict | Notes |
|----|---------|-------|
| AP1 | clean | All per-class qty divisors in this audit set are correctly scoped: `post_so_ship` uses per-class signed SUM on `transfers.qty` filtered to `v_val_acct IN (debit, credit)` (line 470–474); `post_standard_cost_roll`'s WIP path reads stock_wip qty (single-class by partition: sku × routing_op only). The other functions don't compute divisors. |
| AP2 | clean | `_post_transfers_apply_event` retains credit-first COALESCE at line 528 (the canonical correct shape, unchanged from Phase 2). `post_so_ship` pre-dispatches on `v_sl.sku_id` (single SKU on the FG line, no COALESCE drift). `wac_periodic_close_hook` / `wac_retroactive_close_hook` mixed-method branches resolve destination SKU on `v_orig.debit_account_id` for ROUTE detection only — recompute still walks credit-side pool (R2 satisfied). |
| AP3 | clean (closed by mig 0091) | (a) **`post_so_ship`** lines 470–486: pool qty/value read for wac_* unit_cost without FOR UPDATE on `v_val_acct` at the read site — **closed by acct-5prc / mig 0091**: `PERFORM 1 FROM accounts WHERE id = v_val_acct FOR UPDATE` immediately precedes the qty SUM read. (b) **`post_standard_cost_roll`** WIP loop lines 328–331: read paired stock_wip qty without FOR UPDATE — **closed by acct-quca / mig 0091**: lock-set extended (Option B) to UNION-in stock_wip(sku, routing_op) accounts when `p_revalue_wip = TRUE`; single id-sorted FOR UPDATE pass covers both inv_value_wip and stock_wip. |
| AP4 | clean | `resolve_standard_cost_at` is gated by `cost_method = 'standard'` arm in every caller in this audit set (post_po_return, post_so_ship, post_po_receipt, post_standard_cost_roll). |
| AP5 | clean | No new shared-pool drain-then-revalue paths. Mixed-method close-hook branches correctly use single-leg routing against component pool (CLAUDE.md R5). |
| AP6 | clean | All variance routings in this audit set either: (a) post against pools that grow (post_standard_cost_roll WIP), (b) absorb tolerance against non-drained pool (post_ap_bill / post_customer_invoice variance_match_tolerance), (c) follow CLAUDE.md R5 single-leg pattern (mixed-method close-hook). |
| AP7 | clean | All inv_value_*  / variance account lookups include `currency = ...` filters. The receipt-period-closed lookup in mig 0089 (line 238–241) keys on date range only but `periods` is currency-agnostic by design. |
| AP8 | clean | All entry-points in this audit set use UNIQUE(idempotency_key) on header table + ON CONFLICT DO NOTHING + fast-path replay. The mig 0085 / 0086 / 0087 / 0089 chain of post_po_return DROP+CREATE re-issues preserve the same idempotency shape across each iteration. No transfers-key-only race surfaces (the acct-du2.3 OSP shape) appear in the new entry-points. |
| AP9 | clean (closed by mig 0091) | New anti-pattern surfaced by this audit refresh. AP9 = document-level audit field (`*_lines.unit_cost` / `cost_method` snapshots) computed pre-lock while the ledger amount is silently recomputed post-lock by `_post_transfers_compute_amount`'s WAC two-pass. Distinct from AP3: AP3 fails the ledger; AP9 leaves the ledger correct but introduces document-vs-ledger drift in audit-trail columns. Both AP3 hits in this addendum (acct-5prc post_so_ship, acct-quca post_standard_cost_roll WIP path) were simultaneously AP9 hits — the same lock-gap fixes (mig 0091) close both. Mapped to **R7** in CLAUDE.md class-confusion checklist. |

**Severity totals (this addendum)**: 0 × bug, 0 × P1, 0 × P2, 2 × P3 (both AP3+AP9 lock-gaps — **closed by mig 0091**), 1 × P4 (cosmetic divide-by-zero in tolerance check — **closed by mig 0091** via explicit zero-baseline arm in post_ap_bill / post_customer_invoice). Other 12 functions clean.

**Headline finding**: the new functions in migrations 0077–0090 are largely well-built — caller-supplied amounts (memos, payments, invoices, returns) bypass the cost-dispatch surface and AP1–AP4 are mostly non-applicable. **The audit surfaced one new anti-pattern (AP9 / R7)**: document-level audit fields snapshot pool reads pre-lock while the ledger silently corrects via the dispatcher's post-lock recompute. The two AP3 sub-issues filed (acct-5prc, acct-quca) were simultaneously AP9 instances — pattern-greppable AP1–AP8 are clean across new entry-points; the audit-trail-vs-ledger drift class was the bug class that slipped through pattern-grep. **Closed by mig 0091** (Tier 1 audit-loop closure 2026-05-05): all three audit findings (acct-5prc / acct-quca / acct-nuw7) shipped in a single migration with regression tests. Codified as AP9 here and R7 in CLAUDE.md so future audits can pattern-grep for it (look for `INTO v_unit_cost ... ; ... INSERT INTO *_lines (..., unit_cost, ...) VALUES (..., v_unit_cost, ...)` shape across post-lock barriers).
