-- acct-ow8 / BOM2 Phase A5 — bom_lines table + self-reference trigger.
--
-- Additive (per the A4 revision strategy): created alongside the
-- existing boms/wo_routing_burdens. New functions in B6+ consume this
-- table; old tables drop in a final cleanup migration after C3.
--
-- bom_lines is the unified line table. One row per BOM component,
-- service, or per-lot charge. The (kind, basis, applies_at_op,
-- fire_at, scrap_pct) tuple captures the per-line semantics:
--
--   kind ∈ {item, service, charge}        — what the line represents
--   basis ∈ {per_unit, per_lot}            — how it scales with WO qty
--   applies_at_op INT                      — which routing op consumes it
--   fire_at ∈ {wo_start, op_arrival}       — when the apply event posts
--   scrap_pct NUMERIC(5,2)                 — planned material loss for items
--
-- CHECK constraints encode the discriminator-to-column-presence rules:
--   - 'item' lines have component_sku_id + component_loc_id + qty_per_parent
--     (and absorption_class_id + std_amount NULL)
--   - 'service' / 'charge' lines have absorption_class_id + std_amount
--     (and component_* + qty_per_parent NULL)
--
-- Plus invariants:
--   - 'item' lines are always per_unit (a component's qty scales with WO qty)
--   - 'charge' lines are always per_lot (a charge is fixed per run)
--   - 'service' lines can be either basis
--   - per_unit lines fire only at op_arrival; per_lot can fire either way
--
-- The cross-table CHECK that component_sku_id ≠ parent_sku_id (would
-- be a self-reference cycle in the BOM) cannot live as a literal CHECK
-- constraint — CHECK can't reference other tables in PG. We implement
-- it as a row-level BEFORE INSERT/UPDATE trigger that raises P0034
-- bom_line_self_reference.

CREATE TABLE bom_lines (
  bom_id              BIGINT NOT NULL REFERENCES bom_headers(id) ON DELETE CASCADE,
  line_no             INT  NOT NULL,
  kind                TEXT NOT NULL CHECK (kind IN ('item', 'service', 'charge')),
  basis               TEXT NOT NULL CHECK (basis IN ('per_unit', 'per_lot')),
  applies_at_op       INT  NOT NULL CHECK (applies_at_op > 0),
  fire_at             TEXT NOT NULL DEFAULT 'op_arrival'
                      CHECK (fire_at IN ('wo_start', 'op_arrival')),
  scrap_pct           NUMERIC(5,2) NOT NULL DEFAULT 0
                      CHECK (scrap_pct >= 0 AND scrap_pct < 100),
  -- 'item' fields:
  component_sku_id    UUID REFERENCES skus(id),
  component_loc_id    UUID REFERENCES locations(id),
  qty_per_parent      BIGINT,
  -- 'service' / 'charge' fields:
  absorption_class_id BIGINT REFERENCES absorption_classes(id),
  std_amount          BIGINT,
  PRIMARY KEY (bom_id, line_no),
  -- discriminator → column-presence
  CHECK (
    (kind = 'item'
     AND component_sku_id IS NOT NULL AND component_loc_id IS NOT NULL
     AND qty_per_parent IS NOT NULL AND qty_per_parent > 0
     AND absorption_class_id IS NULL AND std_amount IS NULL)
    OR
    (kind IN ('service', 'charge')
     AND component_sku_id IS NULL AND component_loc_id IS NULL AND qty_per_parent IS NULL
     AND absorption_class_id IS NOT NULL
     AND std_amount IS NOT NULL AND std_amount >= 0)
  ),
  -- basis × kind invariant
  CHECK (
    (kind = 'item'    AND basis = 'per_unit')
 OR (kind = 'charge'  AND basis = 'per_lot')
 OR (kind = 'service')
  ),
  -- per_unit lines fire only at op_arrival; per_lot can fire either way
  CHECK (basis = 'per_lot' OR fire_at = 'op_arrival')
);

CREATE INDEX bom_lines_applies_at_op
  ON bom_lines (bom_id, applies_at_op);

CREATE INDEX bom_lines_component
  ON bom_lines (component_sku_id) WHERE component_sku_id IS NOT NULL;

CREATE INDEX bom_lines_class
  ON bom_lines (absorption_class_id) WHERE absorption_class_id IS NOT NULL;

CREATE INDEX bom_lines_fire_at_wo_start
  ON bom_lines (bom_id) WHERE fire_at = 'wo_start';

-- Self-reference guard: a 'item' line's component_sku_id cannot equal
-- the bom_header's parent_sku_id. CHECK can't query other tables, so
-- we use a trigger.

CREATE OR REPLACE FUNCTION _bom_line_self_reference_guard() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
  v_parent UUID;
BEGIN
  IF NEW.kind <> 'item' OR NEW.component_sku_id IS NULL THEN
    RETURN NEW;
  END IF;
  SELECT parent_sku_id INTO v_parent FROM bom_headers WHERE id = NEW.bom_id;
  IF v_parent = NEW.component_sku_id THEN
    RAISE EXCEPTION
      'bom_line_self_reference: bom %, line %: component_sku_id (%) equals parent_sku_id',
      NEW.bom_id, NEW.line_no, NEW.component_sku_id
      USING ERRCODE = 'P0034';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER bom_line_self_reference_check
  BEFORE INSERT OR UPDATE ON bom_lines
  FOR EACH ROW EXECUTE FUNCTION _bom_line_self_reference_guard();

COMMENT ON TABLE bom_lines IS
  'Unified BOM lines: items (physical components), services (per-unit '
  'or per-lot absorption), charges (fixed per-lot). Discriminated by '
  'kind. fire_at controls when the apply event posts. scrap_pct '
  'gross-up applies to item qty at consumption. acct-ow8.';

COMMENT ON FUNCTION _bom_line_self_reference_guard() IS
  'Trigger function preventing a bom_line item from referencing its '
  'own parent_sku as the component (cycle in the BOM). Raises P0034 '
  'bom_line_self_reference. acct-ow8.';
