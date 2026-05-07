-- acct-7t4 sidecar: rename bom_lines.scrap_pct → bom_lines.yield_pct.
--
-- WHY: yield is the conventional BOM term for planned material survival
-- (D365 / Oracle / process industries lead with yield; "scrap factor"
-- is a SAP idiosyncrasy). The column is currently planning metadata
-- only — mig 0061 (acct-6jq) explicitly removed all gross-up at
-- emission and per-op flow — so the rename is a column-name flip plus
-- a value flip (yield_pct = 100 - scrap_pct), with no live ledger
-- consumer to retune. The future BOM rollup tool (still unbuilt) will
-- read `1/(yield_pct/100)` instead of `1/(1 - scrap_pct/100)`. Same
-- math, idiomatic name.
--
-- Surface area:
--   1. Rename column on `bom_lines`, drop old CHECK, add new CHECK with
--      yield semantics (yield_pct > 0 AND yield_pct <= 100), set
--      default to 100, flip existing values.
--   2. Re-create `_wo_explode_bom` (RETURNS TABLE column rename;
--      CREATE OR REPLACE can't change column names → DROP + CREATE).
--   3. Refresh COMMENTs on bom_lines + skus.yield_mode to use the new
--      term.
--
-- The old CHECK constraint is auto-named `bom_lines_scrap_pct_check`.

-- Step 1: drop old CHECK, rename column, flip values, set new default,
-- add new CHECK.
ALTER TABLE bom_lines DROP CONSTRAINT bom_lines_scrap_pct_check;

ALTER TABLE bom_lines RENAME COLUMN scrap_pct TO yield_pct;

ALTER TABLE bom_lines ALTER COLUMN yield_pct SET DEFAULT 100;

UPDATE bom_lines SET yield_pct = 100 - yield_pct;

ALTER TABLE bom_lines ADD CONSTRAINT bom_lines_yield_pct_check
  CHECK (yield_pct > 0 AND yield_pct <= 100);

COMMENT ON TABLE bom_lines IS
  'Unified BOM lines: items (physical components), services (per-unit '
  'or per-lot absorption), charges (fixed per-lot). Discriminated by '
  'kind. fire_at controls when the apply event posts. yield_pct '
  '(default 100) is per-line planned material yield — planning '
  'metadata only; rollup behavior is selected by skus.yield_mode. '
  'acct-ow8 (renamed from scrap_pct in acct-7t4 sidecar).';

-- Step 2: re-create _wo_explode_bom with yield_pct in the RETURNS TABLE
-- and SELECT projections. Body is otherwise identical to mig 0050.
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
  yield_pct            NUMERIC,
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
      bl.kind, bl.basis, bl.applies_at_op, bl.fire_at, bl.yield_pct,
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
      child.fire_at, child.yield_pct,
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
      bl.kind, bl.basis, bl.applies_at_op, bl.fire_at, bl.yield_pct,
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
      child.fire_at, child.yield_pct,
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
    walk.kind, walk.basis, walk.applies_at_op, walk.fire_at, walk.yield_pct,
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
  'BOM at business_date. acct-e5x (yield_pct rename in acct-7t4 sidecar).';

-- Step 3: refresh COMMENT on skus.yield_mode so its formula references
-- yield_pct in idiomatic form.
COMMENT ON COLUMN skus.yield_mode IS
  'Whether bom_lines.yield_pct factors into the parent''s standard cost '
  'rollup. ''plan_only'' (default): yield_pct is planning metadata, std '
  'cost = literal Σ. ''absorbed'': rollup tool inflates parent std '
  'cost by 1/(yield_pct/100) per item line. Pool flow (emission, op_move) '
  'is LITERAL in both modes — yield_pct never affects per-event qty. '
  'acct-6jq (yield_pct rename in acct-7t4 sidecar).';
