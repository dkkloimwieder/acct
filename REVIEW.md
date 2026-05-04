# REVIEW.md — acct-du2 codebase audit

Pattern-grep audit (Phase 1) over `db/migrations/*.up.sql` for the eight anti-patterns AP1–AP8 from `acct-du2`. Latest migration audited: `0070_wac_retroactive_wip`. Function inventory: 15 entry points + ~23 helpers (see CLAUDE.md "Repository status" for the full set). This document is the deliverable for Phase 1; Phase 2 (structural per-function audit) extends it with the 7-question per-function notes.

## Anti-pattern reference

- **AP1** stock_available used as a per-class qty divisor.
- **AP2** debit-first SKU resolution on flagging or cost_method dispatch paths.
- **AP3** pool read (`SELECT debits_total - credits_total`) without prior `FOR UPDATE` on the same account.
- **AP4** `qty * resolve_standard_cost_at(...)` not gated by a `cost_method = 'standard'` CASE arm.
- **AP5** mutation of a shared pool without solo-occupancy gate.
- **AP6** variance routing through a debit-normal pool that the same path drained to 0.
- **AP7** `inv_value_*` read not filtered by `currency`.
- **AP8** idempotency-replay check that happens only before `FOR UPDATE` on the primary lock target (acct-69p pattern).

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
