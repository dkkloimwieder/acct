-- acct-qkm / BOM2 Phase B3 — _wo_resolve_bom_for(wo_id, business_date) resolver.
--
-- One-stop resolver for "which BOM does this WO use". Two paths:
--
--   1. work_orders.bom_id is SET → caller pinned a specific BOM at WO
--      creation. Use it as-is. Don't apply effectivity gates — caller
--      may have pinned a draft or future-effective BOM intentionally
--      (testing scenarios, alternate vendor for a single WO, etc.).
--
--   2. work_orders.bom_id is NULL → use the primary active BOM for
--      parent_sku_id at p_business_date. Delegates to bom_header_at
--      (B2) with alternate_no = 1.
--
-- Errors:
--   P0026 wo_invalid — WO not found, or pinned bom_id missing.
--   P0033 bom_header_resolution_invalid — fall-through from bom_header_at
--   when no primary active BOM exists.

CREATE OR REPLACE FUNCTION _wo_resolve_bom_for(p_wo_id UUID, p_business_date DATE)
RETURNS bom_headers
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  v_wo   work_orders%ROWTYPE;
  v_bom  bom_headers%ROWTYPE;
BEGIN
  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  IF v_wo.bom_id IS NOT NULL THEN
    SELECT * INTO v_bom FROM bom_headers WHERE id = v_wo.bom_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION
        'wo_invalid: WO % has bom_id=% but bom_header missing (FK should prevent this)',
        p_wo_id, v_wo.bom_id USING ERRCODE = 'P0026';
    END IF;
    RETURN v_bom;
  END IF;

  RETURN bom_header_at(v_wo.parent_sku_id, 1, p_business_date);
END;
$$;

COMMENT ON FUNCTION _wo_resolve_bom_for(UUID, DATE) IS
  'Resolves the bom_header to use for a WO at business_date. Honors '
  'work_orders.bom_id if set; falls back to bom_header_at(parent, 1, '
  'business_date) for the primary. acct-qkm.';
