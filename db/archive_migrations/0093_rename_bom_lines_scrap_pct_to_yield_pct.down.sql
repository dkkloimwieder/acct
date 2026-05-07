-- Down: revert yield_pct → scrap_pct on bom_lines and _wo_explode_bom.

ALTER TABLE bom_lines DROP CONSTRAINT bom_lines_yield_pct_check;

ALTER TABLE bom_lines RENAME COLUMN yield_pct TO scrap_pct;

ALTER TABLE bom_lines ALTER COLUMN scrap_pct SET DEFAULT 0;

UPDATE bom_lines SET scrap_pct = 100 - scrap_pct;

ALTER TABLE bom_lines ADD CONSTRAINT bom_lines_scrap_pct_check
  CHECK (scrap_pct >= 0 AND scrap_pct < 100);

COMMENT ON TABLE bom_lines IS
  'Unified BOM lines: items (physical components), services (per-unit '
  'or per-lot absorption), charges (fixed per-lot). Discriminated by '
  'kind. fire_at controls when the apply event posts. scrap_pct '
  'gross-up applies to item qty at consumption. acct-ow8.';

DROP FUNCTION _wo_explode_bom(BIGINT, DATE);

CREATE FUNCTION _wo_explode_bom(
  p_bom_id        BIGINT,
  p_business_date DATE
) RETURNS TABLE(
  source_bom_id        BIGINT,
  source_line_no       INT,
  depth                INT,
  kind                 TEXT,
  basis                TEXT,
  applies_at_op        INT,
  fire_at              TEXT,
  scrap_pct            NUMERIC,
  component_sku_id     UUID,
  component_loc_id     UUID,
  qty_per_parent       BIGINT,
  absorption_class_id  BIGINT,
  std_amount           BIGINT
)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  v_orphan_phantom UUID;
  v_overflow       BOOLEAN;
BEGIN
  IF p_bom_id IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: _wo_explode_bom called with NULL bom_id'
      USING ERRCODE = 'P0026';
  END IF;

  WITH RECURSIVE walk AS (
    SELECT
      bl.bom_id                              AS source_bom_id,
      bl.line_no                             AS source_line_no,
      1                                      AS depth,
      bl.kind, bl.basis, bl.applies_at_op, bl.fire_at, bl.scrap_pct,
      bl.component_sku_id, bl.component_loc_id, bl.qty_per_parent,
      bl.absorption_class_id, bl.std_amount,
      COALESCE(s.is_phantom, FALSE)          AS comp_is_phantom,
      ph.id                                  AS phantom_bom_id
    FROM bom_lines bl
    LEFT JOIN skus s ON s.id = bl.component_sku_id
    LEFT JOIN LATERAL (
      SELECT bh.id FROM bom_headers bh
       WHERE bh.parent_sku_id = bl.component_sku_id
         AND bh.alternate_no  = 1
         AND bh.is_primary
         AND bh.status        = 'active'
         AND bh.effective_at <= (p_business_date::TIMESTAMPTZ + INTERVAL '1 day')
         AND bh.obsolete_at  >  p_business_date::TIMESTAMPTZ
       LIMIT 1
    ) ph ON COALESCE(s.is_phantom, FALSE)
    WHERE bl.bom_id = p_bom_id
    UNION ALL
    SELECT
      child.bom_id, child.line_no,
      parent.depth + 1,
      child.kind, child.basis,
      parent.applies_at_op,
      child.fire_at, child.scrap_pct,
      child.component_sku_id, child.component_loc_id,
      child.qty_per_parent * parent.qty_per_parent,
      child.absorption_class_id,
      CASE
        WHEN child.basis = 'per_unit' AND child.std_amount IS NOT NULL
          THEN child.std_amount * parent.qty_per_parent
        ELSE child.std_amount
      END,
      COALESCE(s2.is_phantom, FALSE),
      ph2.id
    FROM walk parent
    JOIN bom_lines child ON child.bom_id = parent.phantom_bom_id
    LEFT JOIN skus s2 ON s2.id = child.component_sku_id
    LEFT JOIN LATERAL (
      SELECT bh.id FROM bom_headers bh
       WHERE bh.parent_sku_id = child.component_sku_id
         AND bh.alternate_no  = 1
         AND bh.is_primary
         AND bh.status        = 'active'
         AND bh.effective_at <= (p_business_date::TIMESTAMPTZ + INTERVAL '1 day')
         AND bh.obsolete_at  >  p_business_date::TIMESTAMPTZ
       LIMIT 1
    ) ph2 ON COALESCE(s2.is_phantom, FALSE)
    WHERE parent.kind            = 'item'
      AND parent.comp_is_phantom
      AND parent.phantom_bom_id IS NOT NULL
      AND parent.depth < 16
  )
  SELECT
    (SELECT walk.component_sku_id FROM walk
      WHERE walk.kind = 'item' AND walk.comp_is_phantom AND walk.phantom_bom_id IS NULL
      LIMIT 1),
    EXISTS (
      SELECT 1 FROM walk
       WHERE walk.depth = 16
         AND walk.kind = 'item'
         AND walk.comp_is_phantom
         AND walk.phantom_bom_id IS NOT NULL
    )
  INTO v_orphan_phantom, v_overflow;

  IF v_orphan_phantom IS NOT NULL THEN
    RAISE EXCEPTION
      'bom_missing: phantom sku=% has no primary active bom_header at business_date=%',
      v_orphan_phantom, p_business_date
      USING ERRCODE = 'P0029';
  END IF;

  IF v_overflow THEN
    RAISE EXCEPTION
      'phantom_recursion_limit: bom_id=% phantom expansion exceeded depth 16 (cycle or excessive nesting)',
      p_bom_id
      USING ERRCODE = 'P0032';
  END IF;

  RETURN QUERY
  WITH RECURSIVE walk AS (
    SELECT
      bl.bom_id                              AS source_bom_id,
      bl.line_no                             AS source_line_no,
      1                                      AS depth,
      bl.kind, bl.basis, bl.applies_at_op, bl.fire_at, bl.scrap_pct,
      bl.component_sku_id, bl.component_loc_id, bl.qty_per_parent,
      bl.absorption_class_id, bl.std_amount,
      COALESCE(s.is_phantom, FALSE)          AS comp_is_phantom,
      ph.id                                  AS phantom_bom_id
    FROM bom_lines bl
    LEFT JOIN skus s ON s.id = bl.component_sku_id
    LEFT JOIN LATERAL (
      SELECT bh.id FROM bom_headers bh
       WHERE bh.parent_sku_id = bl.component_sku_id
         AND bh.alternate_no  = 1
         AND bh.is_primary
         AND bh.status        = 'active'
         AND bh.effective_at <= (p_business_date::TIMESTAMPTZ + INTERVAL '1 day')
         AND bh.obsolete_at  >  p_business_date::TIMESTAMPTZ
       LIMIT 1
    ) ph ON COALESCE(s.is_phantom, FALSE)
    WHERE bl.bom_id = p_bom_id
    UNION ALL
    SELECT
      child.bom_id, child.line_no,
      parent.depth + 1,
      child.kind, child.basis,
      parent.applies_at_op,
      child.fire_at, child.scrap_pct,
      child.component_sku_id, child.component_loc_id,
      child.qty_per_parent * parent.qty_per_parent,
      child.absorption_class_id,
      CASE
        WHEN child.basis = 'per_unit' AND child.std_amount IS NOT NULL
          THEN child.std_amount * parent.qty_per_parent
        ELSE child.std_amount
      END,
      COALESCE(s2.is_phantom, FALSE),
      ph2.id
    FROM walk parent
    JOIN bom_lines child ON child.bom_id = parent.phantom_bom_id
    LEFT JOIN skus s2 ON s2.id = child.component_sku_id
    LEFT JOIN LATERAL (
      SELECT bh.id FROM bom_headers bh
       WHERE bh.parent_sku_id = child.component_sku_id
         AND bh.alternate_no  = 1
         AND bh.is_primary
         AND bh.status        = 'active'
         AND bh.effective_at <= (p_business_date::TIMESTAMPTZ + INTERVAL '1 day')
         AND bh.obsolete_at  >  p_business_date::TIMESTAMPTZ
       LIMIT 1
    ) ph2 ON COALESCE(s2.is_phantom, FALSE)
    WHERE parent.kind            = 'item'
      AND parent.comp_is_phantom
      AND parent.phantom_bom_id IS NOT NULL
      AND parent.depth < 16
  )
  SELECT
    walk.source_bom_id, walk.source_line_no, walk.depth,
    walk.kind, walk.basis, walk.applies_at_op, walk.fire_at, walk.scrap_pct,
    walk.component_sku_id, walk.component_loc_id, walk.qty_per_parent,
    walk.absorption_class_id, walk.std_amount
  FROM walk
  WHERE NOT (walk.kind = 'item' AND walk.comp_is_phantom)
  ORDER BY walk.source_bom_id, walk.source_line_no, walk.depth;
END;
$$;

COMMENT ON FUNCTION _wo_explode_bom(BIGINT, DATE) IS
  'Recursively expands phantom item-lines in a BOM into a flat row set. '
  'qty_per_parent and per_unit std_amount are multiplied through the path. '
  'applies_at_op is inherited from the outermost parent. Cap at 16 levels '
  '(P0032 if exceeded). P0029 if any phantom child has no primary active '
  'BOM at business_date. acct-e5x.';

COMMENT ON COLUMN skus.yield_mode IS
  'Whether bom_lines.scrap_pct factors into the parent''s standard cost '
  'rollup. ''plan_only'' (default): scrap_pct is planning metadata, std '
  'cost = literal Σ. ''absorbed'': rollup tool inflates parent std '
  'cost by 1/(1-scrap_pct/100) per item line. Pool flow (emission, op_move) '
  'is LITERAL in both modes — scrap_pct never affects per-event qty. '
  'acct-6jq.';
