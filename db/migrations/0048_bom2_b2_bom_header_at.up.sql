-- acct-d3r / BOM2 Phase B2 — bom_header_at(parent_sku_id, alternate_no, business_date) resolver.
--
-- Resolves "the active bom_header for (parent_sku_id, alternate_no) at
-- business_date" — exactly one row per (sku, alternate, business_date)
-- under the bom_headers_active partial UNIQUE index plus the
-- effective_at < obsolete_at invariant.
--
-- Returns the bom_headers ROW (full record). Callers can use the id,
-- code, eco_id, etc. as needed.
--
-- Errors:
--   P0033 bom_header_resolution_invalid — zero matches OR more than one
--   match. (More-than-one would indicate a constraint violation slipping
--   past bom_headers_primary; included as defensive.)

CREATE OR REPLACE FUNCTION bom_header_at(
  p_parent_sku_id UUID,
  p_alternate_no  INT,
  p_business_date DATE
) RETURNS bom_headers
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  v_count INT;
  v_row   bom_headers%ROWTYPE;
BEGIN
  SELECT COUNT(*)::INT INTO v_count
    FROM bom_headers
   WHERE parent_sku_id = p_parent_sku_id
     AND alternate_no  = p_alternate_no
     AND status        = 'active'
     AND effective_at <= (p_business_date::TIMESTAMPTZ + INTERVAL '1 day')
     AND obsolete_at  >  p_business_date::TIMESTAMPTZ;

  IF v_count = 0 THEN
    RAISE EXCEPTION
      'bom_header_resolution_invalid: no active bom_header for parent_sku=%, '
      'alternate_no=%, business_date=%',
      p_parent_sku_id, p_alternate_no, p_business_date
      USING ERRCODE = 'P0033';
  END IF;

  IF v_count > 1 THEN
    RAISE EXCEPTION
      'bom_header_resolution_invalid: % active bom_headers for parent_sku=%, '
      'alternate_no=%, business_date=% (constraint violation)',
      v_count, p_parent_sku_id, p_alternate_no, p_business_date
      USING ERRCODE = 'P0033';
  END IF;

  SELECT * INTO v_row FROM bom_headers
   WHERE parent_sku_id = p_parent_sku_id
     AND alternate_no  = p_alternate_no
     AND status        = 'active'
     AND effective_at <= (p_business_date::TIMESTAMPTZ + INTERVAL '1 day')
     AND obsolete_at  >  p_business_date::TIMESTAMPTZ;

  RETURN v_row;
END;
$$;

COMMENT ON FUNCTION bom_header_at(UUID, INT, DATE) IS
  'Resolves the active bom_header for (parent_sku_id, alternate_no) at '
  'business_date. P0033 if zero or >1 matches. acct-d3r.';
