-- acct-yqt / BOM2 Phase B12 — post_eco_approve workflow.
--
-- Engineering Change Order approval. An ECO points at one or more
-- new bom_headers in 'draft' status. On approval, this function:
--   1. Stamps ECO: status='approved', approved_by, approved_at, effective_at
--   2. For each draft bom_header attached: activates it
--      (status='active', effective_at = ECO.effective_at)
--   3. For each (parent_sku_id, alternate_no) tuple touched: obsoletes
--      the prior active bom_header (status='obsolete',
--      obsolete_at = ECO.effective_at)
--
-- The obsolete-old-then-activate-new ordering avoids a transient
-- duplicate at the bom_headers_primary partial UNIQUE index when both
-- old and new rows have is_primary=TRUE.
--
-- Errors:
--   P0031 eco_invalid_state — ECO not found, status not 'draft', no
--     attached bom_headers, or NULL effective_at/approved_by argument.

CREATE OR REPLACE FUNCTION post_eco_approve(
  p_eco_id        BIGINT,
  p_effective_at  TIMESTAMPTZ,
  p_approved_by   UUID
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_eco        engineering_change_orders%ROWTYPE;
  v_new_count  INT;
  v_new        bom_headers%ROWTYPE;
BEGIN
  IF p_eco_id IS NULL OR p_effective_at IS NULL OR p_approved_by IS NULL THEN
    RAISE EXCEPTION
      'eco_invalid_state: post_eco_approve requires non-null id, effective_at, approved_by'
      USING ERRCODE = 'P0031';
  END IF;

  SELECT * INTO v_eco FROM engineering_change_orders
   WHERE id = p_eco_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'eco_invalid_state: ECO % not found', p_eco_id
      USING ERRCODE = 'P0031';
  END IF;
  IF v_eco.status <> 'draft' THEN
    RAISE EXCEPTION
      'eco_invalid_state: ECO % status=% not draft (cannot approve)',
      p_eco_id, v_eco.status USING ERRCODE = 'P0031';
  END IF;

  SELECT COUNT(*) INTO v_new_count
    FROM bom_headers WHERE eco_id = p_eco_id;
  IF v_new_count = 0 THEN
    RAISE EXCEPTION
      'eco_invalid_state: ECO % has no bom_headers attached',
      p_eco_id USING ERRCODE = 'P0031';
  END IF;

  -- Stamp ECO approved.
  UPDATE engineering_change_orders
     SET status       = 'approved',
         approved_by  = p_approved_by,
         approved_at  = clock_timestamp(),
         effective_at = p_effective_at
   WHERE id = p_eco_id;

  -- For each new bom_header attached to this ECO:
  --   (a) obsolete the prior active row(s) for the same (parent, alt)
  --   (b) activate the new row
  -- (a) first to avoid bom_headers_primary index transient duplicate.
  FOR v_new IN
    SELECT * FROM bom_headers WHERE eco_id = p_eco_id ORDER BY id
  LOOP
    UPDATE bom_headers
       SET obsolete_at = p_effective_at,
           status      = 'obsolete'
     WHERE parent_sku_id = v_new.parent_sku_id
       AND alternate_no  = v_new.alternate_no
       AND id           <> v_new.id
       AND status        = 'active';

    UPDATE bom_headers
       SET effective_at = p_effective_at,
           status       = 'active'
     WHERE id = v_new.id;
  END LOOP;

  RETURN p_eco_id;
END;
$$;

COMMENT ON FUNCTION post_eco_approve(BIGINT, TIMESTAMPTZ, UUID) IS
  'Approves an ECO: stamps approved/approved_by/approved_at/effective_at, '
  'activates attached draft bom_headers, and obsoletes the prior active '
  'rows for each touched (parent_sku, alternate_no). P0031 on bad state. '
  'acct-yqt (BOM2 B12).';
