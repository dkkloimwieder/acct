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

## Phase 2 starting points

For each function listed above with a non-`n/a` cell, Phase 2 will append the 7-question structural notes (scope, value-pool reads, qty-divisor reads, cost_method dispatches, document-sharing assumptions, currency/period boundaries, post-write invariants). Highest-risk order matches the matrix top-to-bottom.

The five sub-issues above are pre-Phase-2 confirmations from the grep pass; Phase 2 may surface additional flags as the structural walks dig deeper than pattern-match recognition allows.
