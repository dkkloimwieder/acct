-- acct-wbj / BOM2 Phase B1 — _wo_apply_reason_for helper (class-id signature).
--
-- Adds an OVERLOAD of the existing _wo_apply_reason_for function (from
-- migration 0038). PG allows multiple functions with the same name and
-- different argument lists.
--
--   Existing (Slice B): _wo_apply_reason_for(p_applied_account_kind account_kind)
--                                                                             → transfer_reason
--   New (this commit):  _wo_apply_reason_for(p_class_id BIGINT, p_basis TEXT)
--                                                                             → transfer_reason
--
-- The new overload is the one B5+ (the new BOM-driven event emitter)
-- will call. Existing _wo_burden_events_for_op (from 0038) keeps using
-- the old account_kind overload — Slice B behavior unchanged.
--
-- Mapping logic (dispatched on absorption_classes.applied_account_kind +
-- the bom_line's basis):
--   labor_applied                              → labor_apply       (canonical)
--   oh_applied                                 → oh_apply          (canonical)
--   absorption_pool + per_unit                 → burden_apply      (generic per-unit)
--   absorption_pool + per_lot                  → lot_charge_apply  (generic per-lot fixed)
--   anything else                              → P0026 (unmapped)
--
-- The "generic" reasons (burden_apply / lot_charge_apply) let new
-- absorption_classes (osp_plating, setup_press, energy_hv, ...) be
-- added at runtime without further migrations — they all hit
-- absorption_pool accounts and ride the generic reasons.
--
-- The canonical kinds (labor_applied, oh_applied) keep their canonical
-- reasons regardless of basis. This preserves Slice B back-compat:
-- a per-lot setup-labor charge against labor_std class still posts
-- with reason='labor_apply', not 'lot_charge_apply'.

CREATE OR REPLACE FUNCTION _wo_apply_reason_for(p_class_id BIGINT, p_basis TEXT)
RETURNS transfer_reason
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  v_applied  account_kind;
  v_code     TEXT;
BEGIN
  SELECT applied_account_kind, code INTO v_applied, v_code
    FROM absorption_classes WHERE id = p_class_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: absorption_class id=% not found', p_class_id
      USING ERRCODE = 'P0026';
  END IF;

  IF p_basis NOT IN ('per_unit', 'per_lot') THEN
    RAISE EXCEPTION 'wo_invalid: bad basis ''%''  for class % (% )', p_basis, p_class_id, v_code
      USING ERRCODE = 'P0026';
  END IF;

  CASE v_applied
    WHEN 'labor_applied'   THEN RETURN 'labor_apply';
    WHEN 'oh_applied'      THEN RETURN 'oh_apply';
    WHEN 'absorption_pool' THEN
      IF p_basis = 'per_lot' THEN
        RETURN 'lot_charge_apply';
      ELSE
        RETURN 'burden_apply';
      END IF;
    ELSE
      RAISE EXCEPTION
        'wo_invalid: absorption_class % (id=%) has applied_account_kind=% with no '
        'transfer_reason mapping. Use labor_applied, oh_applied, or absorption_pool. '
        'For new burden types, use absorption_pool with per_unit (→ burden_apply) '
        'or per_lot (→ lot_charge_apply).',
        v_code, p_class_id, v_applied
        USING ERRCODE = 'P0026';
  END CASE;
END;
$$;

COMMENT ON FUNCTION _wo_apply_reason_for(BIGINT, TEXT) IS
  'Maps absorption_class_id × basis → transfer_reason for apply events. '
  'Overload of the Slice B _wo_apply_reason_for(account_kind). The '
  '(class_id, basis) signature is what B5+ (BOM-driven emitter) calls. '
  'P0026 if class missing or applied_account_kind unmapped. acct-wbj.';
