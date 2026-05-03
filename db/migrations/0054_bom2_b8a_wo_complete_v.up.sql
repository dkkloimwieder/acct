-- acct-n7p / BOM2 Phase B8 (part a) — add wo_complete_v transfer_reason.
--
-- Rationale: the dispatcher's `standard` branch on cost-event reasons
-- (op_move / scrap / wo_complete / so_ship) auto-prices via
-- qty × resolve_standard_cost_at(sku, business_date). For new-path
-- post_wo_complete with multi-output, we need to drive value-leg
-- amounts via wo_outputs.allocation_pct (caller-provided), which the
-- dispatcher's auto-pricing would override.
--
-- Mirrors the existing op_move / op_move_v pattern: caller-supplied
-- amount stands on `wo_complete_v`; dispatcher leaves it untouched.
--
-- ALTER TYPE ADD VALUE must commit before any function references
-- the new label, hence this is a separate migration from the function
-- rewrite (0055).

ALTER TYPE transfer_reason ADD VALUE 'wo_complete_v';
