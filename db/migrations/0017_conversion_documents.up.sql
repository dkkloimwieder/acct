-- 0017_conversion_documents — Slice B + BOM2 + by-products + OSP +
-- post_ap_bill disposal_match extension.
--
-- Body sources (in archive_migrations/):
--   * 0037 (acct-b82) — work_orders / wo_routings initial schema
--   * 0038 (acct-b82) — wo_events table
--   * 0040 (acct-89i) — skus.default_lot_size / is_phantom / consumption_policy
--                       (already applied in 0006_reference_stubs)
--   * 0041 (acct-98e) — engineering_change_orders
--   * 0042 (acct-iku) — absorption_classes
--   * 0043 (acct-z49) — bom_headers
--   * 0044 (acct-ow8) — bom_lines + self-reference trigger
--   * 0045 (acct-zqy) — work_orders.bom_id
--   * 0046 (acct-e50) — wo_outputs
--   * 0047 (acct-wbj) — _wo_apply_reason_for(class_id, basis)
--   * 0048 (acct-d3r) — bom_header_at → renamed _bom_header_at
--   * 0049 (acct-qkm) — _wo_resolve_bom_for
--   * 0050 (acct-e5x) — _wo_explode_bom phantom recursion
--   * 0056 (acct-7he) — wo_events 'close_unproduced' kind
--   * 0058 (acct-yqt) — post_eco_approve
--   * 0060 (acct-0kl) — consumption_policy gate trigger
--   * 0061 (acct-6jq) — yield_mode (column on skus already in 0006);
--                       _wo_emit_bom_lines drop CEIL gross-up
--   * 0062 (acct-jtt) — pure-new-path post_wo_start / post_op_move /
--                       post_wo_complete; legacy boms / wo_routing_burdens
--                       are NOT created here (BOM2 is the only path)
--   * 0063 (acct-wig) — wac_perpetual on WIP gate lift
--   * 0064/0065/0070 — wac on WIP tier 1/2/3 (post_op_move / post_wo_complete)
--   * 0066/0067 (acct-24b/-7py) — rm_issue_to_wo per-component cost dispatch
--                                  in _wo_emit_bom_lines
--   * 0068 (acct-69e) — solo-at-pool gate on post_wo_complete pre-balance
--                        and residual sweep
--   * 0072 (acct-du2.10) — solo-at-pool gate on post_wo_close_unproduced
--   * 0073/0075 (acct-du2.1/.2/.5/.6/.7) — extend FOR UPDATE lock set
--   * 0074 (acct-du2.3) — OSP idempotency dual-check
--   * 0077 (acct-7eo) — mixed parent/component cost methods in
--                       _wo_emit_bom_lines (single-leg variance routing
--                       at close hook lives in 0015)
--   * 0093 (acct-93rename) — bom_lines.scrap_pct → yield_pct (column name
--                            renamed throughout)
--   * 0096 (acct-ksnh) — bom_by_products
--   * 0097 (acct-v5r6) — wo_by_products + immutability trigger
--   * 0098 (acct-u1n9) — by-products pre-pass nrv_credit / negligible
--   * 0099 (acct-6g47) — disposal_cost handling
--   * 0100 (acct-3yno) — disposal_match kind on post_ap_bill
--   * 0101 (acct-a41h) — yield variance for by-products
--
-- Naming unifications baked in:
--   transfers              → posting_lines
--   transfer_reason        → posting_line_reason
--   _post_transfers_*      → _post_posting_lines_*
--   post_transfers         → post_posting_lines
--   bom_header_at          → _bom_header_at
--   resolve_standard_cost_at → _resolve_standard_cost_at
--   stock_consigned_at_vendor → stock_consigned
--   variance_wac_period    → variance_wac_periodic
--   bom_lines.scrap_pct    → bom_lines.yield_pct
--   document_kind 'wo_event' generic → per-event values
--                                       'wo_start' / 'op_move' / 'wo_complete' /
--                                       'scrap' / 'wo_close_unproduced'
--                                       'osp_ship' / 'osp_receive'
--   bom_headers.effective_at / .obsolete_at → DATE (was TIMESTAMPTZ)
--   engineering_change_orders.effective_at  → DATE (was TIMESTAMPTZ)
--   _wo_emit_bom_lines gains p_document_kind parameter so callers thread
--   their per-event document_kind through.

-- ============================================================
-- 1. absorption_classes
-- ============================================================

CREATE TABLE absorption_classes (
  id                    BIGSERIAL PRIMARY KEY,
  code                  TEXT UNIQUE NOT NULL,
  display_name          TEXT NOT NULL,
  applied_account_kind  account_kind NOT NULL,
  expense_account_kind  account_kind,
  description           TEXT,
  is_active             BOOLEAN NOT NULL DEFAULT TRUE,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (expense_account_kind IS NULL
         OR applied_account_kind <> expense_account_kind)
);

CREATE INDEX absorption_classes_active_code
  ON absorption_classes (code) WHERE is_active;

INSERT INTO absorption_classes
  (code, display_name, applied_account_kind, expense_account_kind, description)
VALUES
  ('labor_std', 'Standard Labor', 'labor_applied', 'labor_expense',
   'Direct labor absorption.'),
  ('oh_std',    'Standard Overhead', 'oh_applied',  NULL,
   'Manufacturing overhead absorption. Companion expense kind opens in '
   'Phase 2 acct-oef BURDENCLOSE.');

-- ============================================================
-- 2. engineering_change_orders
-- ============================================================

CREATE TABLE engineering_change_orders (
  id              BIGSERIAL PRIMARY KEY,
  code            TEXT UNIQUE NOT NULL,
  description     TEXT NOT NULL,
  status          TEXT NOT NULL DEFAULT 'draft'
                  CHECK (status IN ('draft', 'approved', 'rejected', 'superseded')),
  requested_by    UUID NOT NULL,
  requested_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  approved_by     UUID,
  approved_at     TIMESTAMPTZ,
  effective_at    DATE,
  rejected_reason TEXT,
  CHECK (
    (status = 'approved'
       AND approved_by IS NOT NULL AND approved_at IS NOT NULL
       AND effective_at IS NOT NULL AND rejected_reason IS NULL)
    OR
    (status = 'rejected'   AND rejected_reason IS NOT NULL)
    OR
    (status IN ('draft', 'superseded'))
  )
);

CREATE INDEX engineering_change_orders_status_active
  ON engineering_change_orders(status)
  WHERE status IN ('draft', 'approved');

CREATE INDEX engineering_change_orders_effective
  ON engineering_change_orders(effective_at)
  WHERE status = 'approved';

-- ============================================================
-- 3. bom_headers
-- ============================================================

CREATE TABLE bom_headers (
  id              BIGSERIAL PRIMARY KEY,
  parent_sku_id   UUID NOT NULL REFERENCES skus(id),
  alternate_no    INT  NOT NULL DEFAULT 1 CHECK (alternate_no >= 1),
  revision_no     TEXT NOT NULL DEFAULT 'A',
  code            TEXT,
  description     TEXT,
  status          TEXT NOT NULL DEFAULT 'active'
                  CHECK (status IN ('draft', 'active', 'obsolete')),
  is_primary      BOOLEAN NOT NULL DEFAULT FALSE,
  effective_at    DATE NOT NULL DEFAULT '-infinity'::DATE,
  obsolete_at     DATE NOT NULL DEFAULT 'infinity'::DATE,
  eco_id          BIGINT REFERENCES engineering_change_orders(id),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (parent_sku_id, alternate_no, revision_no),
  CHECK (effective_at < obsolete_at)
);

CREATE INDEX bom_headers_parent_alt
  ON bom_headers (parent_sku_id, alternate_no);

CREATE INDEX bom_headers_active
  ON bom_headers (parent_sku_id, alternate_no, effective_at, obsolete_at)
  WHERE status = 'active';

CREATE UNIQUE INDEX bom_headers_primary
  ON bom_headers (parent_sku_id, alternate_no)
  WHERE is_primary AND status = 'active';

CREATE INDEX bom_headers_eco
  ON bom_headers (eco_id) WHERE eco_id IS NOT NULL;

-- ============================================================
-- 4. bom_lines
-- ============================================================

CREATE TABLE bom_lines (
  bom_id              BIGINT NOT NULL REFERENCES bom_headers(id) ON DELETE CASCADE,
  line_no             INT  NOT NULL,
  kind                TEXT NOT NULL CHECK (kind IN ('item', 'service', 'charge')),
  basis               TEXT NOT NULL CHECK (basis IN ('per_unit', 'per_lot')),
  applies_at_op       INT  NOT NULL CHECK (applies_at_op > 0),
  fire_at             TEXT NOT NULL DEFAULT 'op_arrival'
                      CHECK (fire_at IN ('wo_start', 'op_arrival')),
  yield_pct           NUMERIC(5,2) NOT NULL DEFAULT 100
                      CHECK (yield_pct > 0 AND yield_pct <= 100),
  component_sku_id    UUID REFERENCES skus(id),
  component_loc_id    UUID REFERENCES locations(id),
  qty_per_parent      BIGINT,
  absorption_class_id BIGINT REFERENCES absorption_classes(id),
  std_amount          BIGINT,
  PRIMARY KEY (bom_id, line_no),
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
  CHECK (
    (kind = 'item'   AND basis = 'per_unit')
    OR (kind = 'charge' AND basis = 'per_lot')
    OR (kind = 'service')
  ),
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

-- ============================================================
-- 5. bom_by_products
-- ============================================================

CREATE TABLE bom_by_products (
  bom_id              BIGINT NOT NULL REFERENCES bom_headers(id) ON DELETE CASCADE,
  by_product_no       INT  NOT NULL,
  output_sku_id       UUID NOT NULL REFERENCES skus(id),
  fg_location_id      UUID NOT NULL REFERENCES locations(id),
  qty_per_parent      NUMERIC(18,6) NOT NULL CHECK (qty_per_parent > 0),
  unit_value          BIGINT NOT NULL,
  treatment           TEXT NOT NULL CHECK (treatment IN
    ('nrv_credit', 'negligible', 'disposal_cost')),
  disposal_basis      TEXT CHECK (disposal_basis IN ('inventoriable', 'period')),
  disposal_vendor_id  UUID REFERENCES vendors(id),
  disposal_expense_account_kind account_kind,
  PRIMARY KEY (bom_id, by_product_no),
  CHECK (
    (treatment = 'nrv_credit'
       AND unit_value > 0
       AND disposal_basis IS NULL
       AND disposal_vendor_id IS NULL
       AND disposal_expense_account_kind IS NULL)
    OR
    (treatment = 'negligible'
       AND unit_value = 0
       AND disposal_basis IS NULL
       AND disposal_vendor_id IS NULL
       AND disposal_expense_account_kind IS NULL)
    OR
    (treatment = 'disposal_cost'
       AND unit_value < 0
       AND disposal_basis IS NOT NULL
       AND disposal_vendor_id IS NOT NULL
       AND (disposal_basis = 'period' OR disposal_expense_account_kind IS NULL))
  )
);

CREATE INDEX bom_by_products_lookup
  ON bom_by_products (bom_id, output_sku_id);

CREATE INDEX bom_by_products_vendor
  ON bom_by_products (disposal_vendor_id)
  WHERE disposal_vendor_id IS NOT NULL;

-- ============================================================
-- 6. work_orders
-- ============================================================

CREATE TABLE work_orders (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  wo_no           TEXT NOT NULL UNIQUE,
  parent_sku_id   UUID NOT NULL REFERENCES skus(id),
  fg_location_id  UUID NOT NULL REFERENCES locations(id),
  qty_target      BIGINT NOT NULL CHECK (qty_target > 0),
  qty_completed   BIGINT NOT NULL DEFAULT 0 CHECK (qty_completed >= 0),
  qty_scrapped    BIGINT NOT NULL DEFAULT 0 CHECK (qty_scrapped >= 0),
  status          TEXT NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft', 'released', 'closed')),
  currency        CHAR(3) NOT NULL,
  bom_id          BIGINT REFERENCES bom_headers(id),
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (qty_completed + qty_scrapped <= qty_target)
);

CREATE INDEX work_orders_parent_sku ON work_orders (parent_sku_id);
CREATE INDEX work_orders_status_open ON work_orders (status)
  WHERE status <> 'closed';
CREATE INDEX work_orders_bom_id ON work_orders (bom_id) WHERE bom_id IS NOT NULL;

-- ============================================================
-- 7. wo_routings
-- ============================================================

CREATE TABLE wo_routings (
  wo_id      UUID NOT NULL REFERENCES work_orders(id),
  routing_op INT  NOT NULL CHECK (routing_op > 0),
  op_name    TEXT NOT NULL,
  PRIMARY KEY (wo_id, routing_op)
);

-- ============================================================
-- 8. wo_outputs
-- ============================================================

CREATE TABLE wo_outputs (
  wo_id              UUID NOT NULL REFERENCES work_orders(id),
  output_no          INT  NOT NULL,
  output_sku_id      UUID NOT NULL REFERENCES skus(id),
  fg_location_id     UUID NOT NULL REFERENCES locations(id),
  qty                BIGINT NOT NULL CHECK (qty > 0),
  allocation_method  TEXT NOT NULL CHECK (
    allocation_method IN ('primary', 'sales_value', 'fixed_ratio', 'market_price')
  ),
  allocation_pct     NUMERIC(5,2)
                     CHECK (allocation_pct IS NULL
                            OR (allocation_pct >= 0 AND allocation_pct <= 100)),
  PRIMARY KEY (wo_id, output_no)
);

CREATE INDEX wo_outputs_output_sku
  ON wo_outputs (output_sku_id);

CREATE INDEX wo_outputs_method
  ON wo_outputs (wo_id, allocation_method);

-- ============================================================
-- 9. wo_events
-- ============================================================

CREATE TABLE wo_events (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  wo_id           UUID NOT NULL REFERENCES work_orders(id),
  event_kind      TEXT NOT NULL
                  CHECK (event_kind IN
                    ('start', 'op_move', 'wo_complete', 'scrap', 'close_unproduced')),
  routing_op_from INT,
  routing_op_to   INT,
  qty             BIGINT CHECK (qty IS NULL OR qty > 0),
  business_date   DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT,
  CHECK (
    (event_kind = 'start'
     AND routing_op_from IS NULL AND routing_op_to IS NULL AND qty IS NULL)
    OR
    (event_kind = 'op_move'
     AND routing_op_from IS NOT NULL AND routing_op_to IS NOT NULL
     AND qty IS NOT NULL)
    OR
    (event_kind = 'wo_complete'
     AND routing_op_from IS NOT NULL AND routing_op_to IS NULL
     AND qty IS NOT NULL)
    OR
    (event_kind = 'scrap'
     AND routing_op_from IS NOT NULL AND routing_op_to IS NULL
     AND qty IS NOT NULL)
    OR
    (event_kind = 'close_unproduced'
     AND routing_op_from IS NULL AND routing_op_to IS NULL AND qty IS NULL)
  )
);

CREATE INDEX wo_events_wo ON wo_events (wo_id);
CREATE INDEX wo_events_posted_at ON wo_events (posted_at);

-- consumption_policy gate (only 'forward' is dispatched today;
-- backflush variants raise P0035 deferred to acct-oi4).

CREATE OR REPLACE FUNCTION _wo_events_check_consumption_policy()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
  v_policy TEXT;
  v_parent UUID;
BEGIN
  IF NEW.event_kind <> 'start' THEN
    RETURN NEW;
  END IF;

  SELECT s.consumption_policy::TEXT, w.parent_sku_id
    INTO v_policy, v_parent
    FROM work_orders w
    JOIN skus s ON s.id = w.parent_sku_id
   WHERE w.id = NEW.wo_id;

  IF v_policy <> 'forward' THEN
    RAISE EXCEPTION
      'consumption_policy_not_implemented: parent_sku=% consumption_policy=% — '
      'only ''forward'' is dispatched today (Phase 2 acct-oi4 BACKFLUSH)',
      v_parent, v_policy
      USING ERRCODE = 'P0035';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER wo_events_check_consumption_policy
  BEFORE INSERT ON wo_events
  FOR EACH ROW
  EXECUTE FUNCTION _wo_events_check_consumption_policy();

-- ============================================================
-- 10. wo_by_products
-- ============================================================

CREATE TABLE wo_by_products (
  wo_id               UUID NOT NULL REFERENCES work_orders(id) ON DELETE CASCADE,
  by_product_no       INT  NOT NULL,
  output_sku_id       UUID NOT NULL REFERENCES skus(id),
  fg_location_id      UUID NOT NULL REFERENCES locations(id),
  planned_qty         BIGINT NOT NULL CHECK (planned_qty > 0),
  actual_qty          BIGINT NOT NULL CHECK (actual_qty >= 0),
  unit_value          BIGINT NOT NULL,
  treatment           TEXT NOT NULL CHECK (treatment IN
    ('nrv_credit', 'negligible', 'disposal_cost')),
  disposal_basis      TEXT CHECK (disposal_basis IN ('inventoriable', 'period')),
  disposal_vendor_id  UUID REFERENCES vendors(id),
  disposal_expense_account_kind account_kind,
  PRIMARY KEY (wo_id, by_product_no),
  CHECK (
    (treatment = 'nrv_credit'
       AND unit_value > 0
       AND disposal_basis IS NULL
       AND disposal_vendor_id IS NULL
       AND disposal_expense_account_kind IS NULL)
    OR
    (treatment = 'negligible'
       AND unit_value = 0
       AND disposal_basis IS NULL
       AND disposal_vendor_id IS NULL
       AND disposal_expense_account_kind IS NULL)
    OR
    (treatment = 'disposal_cost'
       AND unit_value < 0
       AND disposal_basis IS NOT NULL
       AND disposal_vendor_id IS NOT NULL
       AND (disposal_basis = 'period' OR disposal_expense_account_kind IS NULL))
  )
);

CREATE INDEX wo_by_products_lookup
  ON wo_by_products (wo_id, output_sku_id);

CREATE INDEX wo_by_products_vendor
  ON wo_by_products (disposal_vendor_id)
  WHERE disposal_vendor_id IS NOT NULL;

CREATE OR REPLACE FUNCTION wo_by_products_planned_qty_immutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.planned_qty IS DISTINCT FROM OLD.planned_qty THEN
    RAISE EXCEPTION
      'wo_by_product_immutable: planned_qty cannot change once set '
      '(wo_id=%, by_product_no=%, old=%, new=%)',
      OLD.wo_id, OLD.by_product_no, OLD.planned_qty, NEW.planned_qty
      USING ERRCODE = 'P0051';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER wo_by_products_planned_qty_immutable_trg
  BEFORE UPDATE ON wo_by_products
  FOR EACH ROW EXECUTE FUNCTION wo_by_products_planned_qty_immutable();

-- ============================================================
-- 11. _bom_header_at — resolver for (parent, alternate, business_date)
-- ============================================================

CREATE OR REPLACE FUNCTION _bom_header_at(
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
     AND effective_at <= p_business_date
     AND obsolete_at  >  p_business_date;

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
     AND effective_at <= p_business_date
     AND obsolete_at  >  p_business_date;

  RETURN v_row;
END;
$$;

-- ============================================================
-- 12. _wo_resolve_bom_for — pinned bom_id or primary fallback
-- ============================================================

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
        'wo_invalid: WO % has bom_id=% but bom_header missing',
        p_wo_id, v_wo.bom_id USING ERRCODE = 'P0026';
    END IF;
    RETURN v_bom;
  END IF;

  RETURN _bom_header_at(v_wo.parent_sku_id, 1, p_business_date);
END;
$$;

-- ============================================================
-- 13. _wo_explode_bom — recursive phantom expansion
-- ============================================================

CREATE OR REPLACE FUNCTION _wo_explode_bom(
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
      bl.bom_id   AS source_bom_id,
      bl.line_no  AS source_line_no,
      1           AS depth,
      bl.kind, bl.basis, bl.applies_at_op, bl.fire_at, bl.yield_pct,
      bl.component_sku_id, bl.component_loc_id, bl.qty_per_parent,
      bl.absorption_class_id, bl.std_amount,
      COALESCE(s.is_phantom, FALSE) AS comp_is_phantom,
      ph.id                          AS phantom_bom_id
    FROM bom_lines bl
    LEFT JOIN skus s ON s.id = bl.component_sku_id
    LEFT JOIN LATERAL (
      SELECT bh.id FROM bom_headers bh
       WHERE bh.parent_sku_id = bl.component_sku_id
         AND bh.alternate_no  = 1
         AND bh.is_primary
         AND bh.status        = 'active'
         AND bh.effective_at <= p_business_date
         AND bh.obsolete_at  >  p_business_date
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
         AND bh.effective_at <= p_business_date
         AND bh.obsolete_at  >  p_business_date
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
      bl.bom_id   AS source_bom_id,
      bl.line_no  AS source_line_no,
      1           AS depth,
      bl.kind, bl.basis, bl.applies_at_op, bl.fire_at, bl.yield_pct,
      bl.component_sku_id, bl.component_loc_id, bl.qty_per_parent,
      bl.absorption_class_id, bl.std_amount,
      COALESCE(s.is_phantom, FALSE) AS comp_is_phantom,
      ph.id                          AS phantom_bom_id
    FROM bom_lines bl
    LEFT JOIN skus s ON s.id = bl.component_sku_id
    LEFT JOIN LATERAL (
      SELECT bh.id FROM bom_headers bh
       WHERE bh.parent_sku_id = bl.component_sku_id
         AND bh.alternate_no  = 1
         AND bh.is_primary
         AND bh.status        = 'active'
         AND bh.effective_at <= p_business_date
         AND bh.obsolete_at  >  p_business_date
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
         AND bh.effective_at <= p_business_date
         AND bh.obsolete_at  >  p_business_date
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

-- ============================================================
-- 14. _wo_apply_reason_for(class_id, basis) — single signature
-- ============================================================
--
-- absorption_class.applied_account_kind × basis → posting_line_reason.
-- canonical: labor_applied → labor_apply, oh_applied → oh_apply.
-- absorption_pool: per_unit → burden_apply, per_lot → lot_charge_apply.

CREATE OR REPLACE FUNCTION _wo_apply_reason_for(p_class_id BIGINT, p_basis TEXT)
RETURNS posting_line_reason
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
    RAISE EXCEPTION 'wo_invalid: bad basis ''%'' for class % (%)',
                    p_basis, p_class_id, v_code
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
        'wo_invalid: absorption_class % (id=%) has applied_account_kind=% '
        'with no posting_line_reason mapping. Use labor_applied, oh_applied, '
        'or absorption_pool.',
        v_code, p_class_id, v_applied
        USING ERRCODE = 'P0026';
  END CASE;
END;
$$;

-- ============================================================
-- 15. _wo_emit_bom_lines — BOM-line event emitter (acct-7eo mig 0077 form)
-- ============================================================
--
-- Per-component cost dispatch:
--   standard component:        v_value = qty × _resolve_standard_cost_at()
--   wac_perpetual component:   running avg from inv_value_raw pool +
--                              per-class qty signed SUM on posting_lines.qty
--   wac_periodic component:    same as wac_perpetual mid-period; close
--                              hook recomputes against final_avg.
--   wac_retroactive component: same as wac_perpetual mid-period; close
--                              hook does chronological replay.
--   fifo / lot:                P0006 (acct-8gg).
--
-- Mixed parent/component cost methods (e.g., standard parent +
-- wac_periodic component) are PERMITTED. The close hook posts single-leg
-- variance through variance_material_mixed against the component pool —
-- destination WIP untouched (CLAUDE.md R5).
--
-- p_document_kind is the per-event document_kind the caller threads
-- through ('wo_start' or 'op_move').

CREATE OR REPLACE FUNCTION _wo_emit_bom_lines(
  p_wo_id          UUID,
  p_bom_id         BIGINT,
  p_routing_op     INT,
  p_qty            BIGINT,
  p_filter         JSONB,
  p_event_id       UUID,
  p_business_date  DATE,
  p_posted_by      UUID,
  p_document_kind  TEXT
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_wo                   work_orders%ROWTYPE;
  v_val_acct_wip         BIGINT;
  v_batch                JSONB := '[]'::JSONB;
  v_line                 RECORD;
  v_filter_kind          TEXT;
  v_filter_basis         TEXT;
  v_filter_fire_at       TEXT;
  v_filter_applies_at_op INT;
  v_adj_qty              BIGINT;
  v_value                BIGINT;
  v_amount               BIGINT;
  v_reason               posting_line_reason;
  v_comp_consumed        BIGINT;
  v_comp_qty_acct        BIGINT;
  v_comp_val_acct        BIGINT;
  v_applied_kind         account_kind;
  v_applied_acct         BIGINT;
  v_comp_std_cost        BIGINT;
  v_comp_cost_method     cost_method;
  v_pool_qty             BIGINT;
  v_pool_value           BIGINT;
  v_unit                 BIGINT;
BEGIN
  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: _wo_emit_bom_lines requires positive p_qty (got %)',
                    p_qty USING ERRCODE = 'P0026';
  END IF;
  IF p_bom_id IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: _wo_emit_bom_lines requires non-NULL p_bom_id'
      USING ERRCODE = 'P0026';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_val_acct_wip FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND currency=v_wo.currency
     AND NOT is_closed;
  IF v_val_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_routing_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_filter_kind          := p_filter->>'kind';
  v_filter_basis         := p_filter->>'basis';
  v_filter_fire_at       := p_filter->>'fire_at';
  v_filter_applies_at_op := NULLIF(p_filter->>'applies_at_op', '')::INT;

  FOR v_line IN
    SELECT exp.*
      FROM _wo_explode_bom(p_bom_id, p_business_date) exp
     WHERE (v_filter_kind          IS NULL OR exp.kind          = v_filter_kind)
       AND (v_filter_basis         IS NULL OR exp.basis         = v_filter_basis)
       AND (v_filter_fire_at       IS NULL OR exp.fire_at       = v_filter_fire_at)
       AND (v_filter_applies_at_op IS NULL OR exp.applies_at_op = v_filter_applies_at_op)
     ORDER BY exp.source_bom_id, exp.source_line_no, exp.depth
  LOOP
    IF v_line.kind = 'item' THEN
      v_adj_qty := p_qty * v_line.qty_per_parent;

      SELECT id INTO v_comp_consumed FROM accounts
       WHERE kind='stock_consumed' AND sku_id=v_line.component_sku_id
         AND ledger_kind='qty' AND NOT is_closed;
      IF v_comp_consumed IS NULL THEN
        RAISE EXCEPTION 'no open stock_consumed account for sku=%',
                        v_line.component_sku_id USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_comp_qty_acct FROM accounts
       WHERE kind='stock_available' AND sku_id=v_line.component_sku_id
         AND location_id=v_line.component_loc_id AND NOT is_closed;
      IF v_comp_qty_acct IS NULL THEN
        RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                        v_line.component_sku_id, v_line.component_loc_id
          USING ERRCODE = 'P0010';
      END IF;

      SELECT id INTO v_comp_val_acct FROM accounts
       WHERE kind='inv_value_raw' AND sku_id=v_line.component_sku_id
         AND location_id=v_line.component_loc_id AND currency=v_wo.currency
         AND NOT is_closed;
      IF v_comp_val_acct IS NULL THEN
        RAISE EXCEPTION 'no open inv_value_raw account for sku=% loc=% ccy=%',
                        v_line.component_sku_id, v_line.component_loc_id, v_wo.currency
          USING ERRCODE = 'P0010';
      END IF;

      SELECT cost_method INTO v_comp_cost_method
        FROM skus WHERE id = v_line.component_sku_id;

      CASE v_comp_cost_method
        WHEN 'standard' THEN
          v_comp_std_cost := _resolve_standard_cost_at(
            v_line.component_sku_id, p_business_date
          );
          v_value := v_adj_qty * v_comp_std_cost;

        WHEN 'wac_perpetual', 'wac_periodic', 'wac_retroactive' THEN
          PERFORM 1 FROM accounts WHERE id = v_comp_val_acct FOR UPDATE;
          SELECT COALESCE(SUM(
            CASE
              WHEN t.debit_account_id  = v_comp_val_acct THEN  t.qty
              WHEN t.credit_account_id = v_comp_val_acct THEN -t.qty
            END
          ), 0)
            INTO v_pool_qty
            FROM posting_lines t
           WHERE v_comp_val_acct IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL;

          IF v_pool_qty <= 0 THEN
            RAISE EXCEPTION
              'rm_issue_empty_pool: % component % at sku=% loc=% has empty '
              'inv_value_raw pool (per-class qty=%); cannot issue % units to WO %',
              v_comp_cost_method, v_line.component_sku_id,
              v_line.component_sku_id, v_line.component_loc_id,
              v_pool_qty, v_adj_qty, p_wo_id
              USING ERRCODE = 'P0010';
          END IF;

          SELECT (debits_total - credits_total) INTO v_pool_value
            FROM accounts WHERE id = v_comp_val_acct;
          v_unit  := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
          v_value := v_adj_qty * v_unit;

        WHEN 'fifo', 'lot' THEN
          RAISE EXCEPTION
            'cost_method_not_implemented: % for component % (acct-8gg)',
            v_comp_cost_method, v_line.component_sku_id
            USING ERRCODE = 'P0006';

        ELSE
          RAISE EXCEPTION
            'unknown cost_method % for component %',
            v_comp_cost_method, v_line.component_sku_id
            USING ERRCODE = 'P0011';
      END CASE;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'rm_issue_to_wo',
        'document_kind',     p_document_kind,
        'document_id',       p_event_id,
        'debit_account_id',  v_comp_consumed,
        'credit_account_id', v_comp_qty_acct,
        'amount',            v_adj_qty,
        'qty',               v_adj_qty,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));

      IF v_value > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'rm_issue_to_wo',
          'document_kind',     p_document_kind,
          'document_id',       p_event_id,
          'debit_account_id',  v_val_acct_wip,
          'credit_account_id', v_comp_val_acct,
          'amount',            v_value,
          'qty',               v_adj_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;

    ELSIF v_line.kind IN ('service', 'charge') THEN
      IF v_line.basis = 'per_unit' THEN
        v_amount := p_qty * v_line.std_amount;
      ELSE
        v_amount := v_line.std_amount;
      END IF;
      IF v_amount <= 0 THEN
        CONTINUE;
      END IF;

      v_reason := _wo_apply_reason_for(v_line.absorption_class_id, v_line.basis);

      SELECT applied_account_kind INTO v_applied_kind FROM absorption_classes
       WHERE id = v_line.absorption_class_id;
      IF v_applied_kind IS NULL THEN
        RAISE EXCEPTION 'wo_invalid: absorption_class id=% not found',
                        v_line.absorption_class_id USING ERRCODE = 'P0026';
      END IF;

      SELECT id INTO v_applied_acct FROM accounts
       WHERE kind = v_applied_kind AND ledger_kind='value'
         AND currency = v_wo.currency AND NOT is_closed
       LIMIT 1;
      IF v_applied_acct IS NULL THEN
        RAISE EXCEPTION 'no open % account for ccy=%',
                        v_applied_kind, v_wo.currency
          USING ERRCODE = 'P0010';
      END IF;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            v_reason,
        'document_kind',     p_document_kind,
        'document_id',       p_event_id,
        'debit_account_id',  v_val_acct_wip,
        'credit_account_id', v_applied_acct,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  RETURN v_batch;
END;
$$;

-- ============================================================
-- 16. post_eco_approve
-- ============================================================

CREATE OR REPLACE FUNCTION post_eco_approve(
  p_eco_id        BIGINT,
  p_effective_at  DATE,
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

  UPDATE engineering_change_orders
     SET status       = 'approved',
         approved_by  = p_approved_by,
         approved_at  = clock_timestamp(),
         effective_at = p_effective_at
   WHERE id = p_eco_id;

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

-- ============================================================
-- 17. post_wo_start
-- ============================================================
--
-- - Idempotency replay before AND after FOR UPDATE on work_orders (R6).
-- - cost_method ∈ {standard, wac_perpetual, wac_periodic, wac_retroactive};
--   fifo/lot raise P0006 (acct-8gg).
-- - Validates BOM ops vs wo_routings; auto-inits wo_outputs (single
--   primary if empty); auto-inits wo_by_products from bom_by_products
--   when caller hasn't pre-populated.
-- - Threads document_kind='wo_start' through both the qty leg and
--   _wo_emit_bom_lines call sites.

CREATE OR REPLACE FUNCTION post_wo_start(
  p_wo_id           UUID,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id     UUID;
  v_event_id        UUID;
  v_wo              work_orders%ROWTYPE;
  v_first_op        INT;
  v_op_count        INT;
  v_cost_method     cost_method;
  v_qty_acct_wip    BIGINT;
  v_void_qty        BIGINT;
  v_val_acct_wip    BIGINT;
  v_bom             bom_headers%ROWTYPE;
  v_bad_op          INT;
  v_alloc_sum       NUMERIC;
  v_batch           JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'draft' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not draft (already started)',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_wo.parent_sku_id;
  IF v_cost_method NOT IN ('standard', 'wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_wo_start does not handle',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  SELECT MIN(routing_op), COUNT(*) INTO v_first_op, v_op_count
    FROM wo_routings WHERE wo_id = p_wo_id;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no routing operations', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_qty_acct_wip FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_first_op AND NOT is_closed;
  IF v_qty_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, v_first_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_void_qty FROM accounts
   WHERE kind='creation_void' AND ledger_kind='qty' AND NOT is_closed;
  IF v_void_qty IS NULL THEN
    RAISE EXCEPTION 'no creation_void(qty) account configured'
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_acct_wip FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_first_op AND currency=v_wo.currency
     AND NOT is_closed;
  IF v_val_acct_wip IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, v_first_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_bom := _wo_resolve_bom_for(p_wo_id, p_business_date);

  SELECT exp.applies_at_op INTO v_bad_op
    FROM _wo_explode_bom(v_bom.id, p_business_date) exp
   WHERE NOT EXISTS (
     SELECT 1 FROM wo_routings wr
      WHERE wr.wo_id = p_wo_id AND wr.routing_op = exp.applies_at_op
   )
   LIMIT 1;
  IF v_bad_op IS NOT NULL THEN
    RAISE EXCEPTION
      'wo_start_op_mismatch: bom_lines reference applies_at_op=% '
      'which is not in wo_routings(wo=%)',
      v_bad_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  PERFORM 1 FROM wo_outputs WHERE wo_id = p_wo_id LIMIT 1;
  IF NOT FOUND THEN
    INSERT INTO wo_outputs (
      wo_id, output_no, output_sku_id, fg_location_id, qty,
      allocation_method, allocation_pct
    ) VALUES (
      p_wo_id, 1, v_wo.parent_sku_id, v_wo.fg_location_id, v_wo.qty_target,
      'primary', 100
    );
  ELSE
    SELECT COALESCE(SUM(allocation_pct), 0)
      INTO v_alloc_sum
      FROM wo_outputs WHERE wo_id = p_wo_id;
    IF v_alloc_sum <> 100 THEN
      RAISE EXCEPTION
        'output_allocation_invalid: wo_outputs(wo=%) allocation_pct sums to % (expected 100)',
        p_wo_id, v_alloc_sum USING ERRCODE = 'P0033';
    END IF;
  END IF;

  -- Snapshot bom_by_products → wo_by_products when caller hasn't pre-populated.
  PERFORM 1 FROM wo_by_products WHERE wo_id = p_wo_id LIMIT 1;
  IF NOT FOUND THEN
    INSERT INTO wo_by_products (
      wo_id, by_product_no, output_sku_id, fg_location_id,
      planned_qty, actual_qty, unit_value, treatment,
      disposal_basis, disposal_vendor_id, disposal_expense_account_kind
    )
    SELECT
      p_wo_id,
      bbp.by_product_no,
      bbp.output_sku_id,
      bbp.fg_location_id,
      ROUND(bbp.qty_per_parent * v_wo.qty_target)::BIGINT AS planned_qty,
      ROUND(bbp.qty_per_parent * v_wo.qty_target)::BIGINT AS actual_qty,
      bbp.unit_value,
      bbp.treatment,
      bbp.disposal_basis,
      bbp.disposal_vendor_id,
      bbp.disposal_expense_account_kind
    FROM bom_by_products bbp
   WHERE bbp.bom_id = v_bom.id
     AND ROUND(bbp.qty_per_parent * v_wo.qty_target) >= 1;
  END IF;

  INSERT INTO wo_events (
    wo_id, event_kind, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'start', p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'wo_start',
    'document_kind',     'wo_start',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_acct_wip,
    'credit_account_id', v_void_qty,
    'amount',            v_wo.qty_target,
    'qty',               v_wo.qty_target,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  v_batch := v_batch || _wo_emit_bom_lines(
    p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
    jsonb_build_object('fire_at', 'wo_start'),
    v_event_id, p_business_date, p_posted_by, 'wo_start'
  );

  v_batch := v_batch || _wo_emit_bom_lines(
    p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
    jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', v_first_op),
    v_event_id, p_business_date, p_posted_by, 'wo_start'
  );

  PERFORM post_posting_lines(v_batch, FALSE);
  UPDATE work_orders SET status = 'released' WHERE id = p_wo_id;
  RETURN p_wo_id;
END;
$$;

-- ============================================================
-- 18. post_op_move
-- ============================================================
--
-- standard parent: per_unit_cum + per_lot_cum from bom_lines (literal,
-- yield_pct is planning metadata only — acct-6jq).
-- wac_* parent: locks BOTH source value pool AND source qty pool in
-- ascending id order (acct-du2.7), then reads both for running avg.
-- First arrival emits per_unit + per_lot lines; subsequent arrival emits
-- per_unit *services only* (items/charges already fired — acct-jtt
-- prevents rework double-issue).

CREATE OR REPLACE FUNCTION post_op_move(
  p_wo_id           UUID,
  p_from_op         INT,
  p_to_op           INT,
  p_qty             BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_event_id         UUID;
  v_wo               work_orders%ROWTYPE;
  v_from_count       INT;
  v_to_count         INT;
  v_qty_from         BIGINT;
  v_qty_to           BIGINT;
  v_val_from         BIGINT;
  v_val_to           BIGINT;
  v_value_amount     BIGINT;
  v_batch            JSONB := '[]'::JSONB;
  v_bom              bom_headers%ROWTYPE;
  v_first_op         INT;
  v_default_lot_size BIGINT;
  v_per_unit_cum     BIGINT;
  v_per_lot_cum      BIGINT;
  v_first_arrival    BOOLEAN;
  v_cost_method      cost_method;
  v_pool_value       BIGINT;
  v_pool_qty         BIGINT;
  v_unit             BIGINT;
  v_lock_first       BIGINT;
  v_lock_second      BIGINT;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: op_move qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;
  IF p_from_op = p_to_op THEN
    RAISE EXCEPTION 'routing_op_invalid: from_op (%) = to_op (%)',
                    p_from_op, p_to_op USING ERRCODE = 'P0028';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not released',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT COUNT(*) INTO v_from_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_from_op;
  IF v_from_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: from_op % not in WO % routing',
                    p_from_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;
  SELECT COUNT(*) INTO v_to_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_to_op;
  IF v_to_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: to_op % not in WO % routing',
                    p_to_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_from_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_from_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_qty_to FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_to_op AND NOT is_closed;
  IF v_qty_to IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_to_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_from_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_from IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_from_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_to FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_to_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_to IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_to_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  v_bom := _wo_resolve_bom_for(p_wo_id, p_business_date);
  SELECT cost_method, default_lot_size INTO v_cost_method, v_default_lot_size
    FROM skus WHERE id = v_wo.parent_sku_id;
  SELECT MIN(routing_op) INTO v_first_op
    FROM wo_routings WHERE wo_id = p_wo_id;

  IF v_cost_method = 'standard' THEN
    SELECT COALESCE(SUM(
      CASE
        WHEN exp.kind = 'item' THEN
          (exp.qty_per_parent
            * _resolve_standard_cost_at(exp.component_sku_id, p_business_date))
        WHEN exp.kind = 'service' AND exp.basis = 'per_unit' THEN exp.std_amount
        ELSE 0
      END
    ), 0) INTO v_per_unit_cum
      FROM _wo_explode_bom(v_bom.id, p_business_date) exp
     WHERE exp.basis = 'per_unit'
       AND exp.applies_at_op <= p_from_op;

    SELECT COALESCE(SUM(exp.std_amount), 0) / v_default_lot_size
      INTO v_per_lot_cum
      FROM _wo_explode_bom(v_bom.id, p_business_date) exp
     WHERE exp.basis = 'per_lot'
       AND (
         exp.fire_at = 'wo_start'
         OR (exp.fire_at = 'op_arrival' AND exp.applies_at_op <= p_from_op)
       );

    v_value_amount := p_qty * (v_per_unit_cum + v_per_lot_cum);

  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
    v_lock_first  := LEAST(v_qty_from, v_val_from);
    v_lock_second := GREATEST(v_qty_from, v_val_from);
    PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
    PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;
    SELECT (debits_total - credits_total) INTO v_pool_value
      FROM accounts WHERE id = v_val_from;
    SELECT (debits_total - credits_total) INTO v_pool_qty
      FROM accounts WHERE id = v_qty_from;

    IF v_pool_qty IS NULL OR v_pool_qty <= 0 THEN
      v_value_amount := 0;
    ELSE
      v_unit := GREATEST(COALESCE(v_pool_value, 0), 0) / v_pool_qty;
      v_value_amount := p_qty * v_unit;
    END IF;

  ELSE
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_op_move does not handle',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  v_first_arrival := NOT EXISTS (
    SELECT 1 FROM wo_events
     WHERE wo_id = p_wo_id
       AND (
         (event_kind = 'op_move' AND routing_op_to = p_to_op)
         OR (event_kind = 'start' AND p_to_op = v_first_op)
       )
  );

  INSERT INTO wo_events (
    wo_id, event_kind, routing_op_from, routing_op_to, qty,
    business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'op_move', p_from_op, p_to_op, p_qty,
    p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'op_move',
    'document_kind',     'op_move',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_to,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  IF v_value_amount > 0 THEN
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'op_move_v',
      'document_kind',     'op_move',
      'document_id',       v_event_id,
      'debit_account_id',  v_val_to,
      'credit_account_id', v_val_from,
      'amount',            v_value_amount,
      'qty',               p_qty,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'posted_by',         p_posted_by
    ));
  END IF;

  IF v_first_arrival THEN
    v_batch := v_batch || _wo_emit_bom_lines(
      p_wo_id, v_bom.id, p_to_op, p_qty,
      jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', p_to_op),
      v_event_id, p_business_date, p_posted_by, 'op_move'
    );
  ELSE
    v_batch := v_batch || _wo_emit_bom_lines(
      p_wo_id, v_bom.id, p_to_op, p_qty,
      jsonb_build_object('fire_at',        'op_arrival',
                         'applies_at_op',  p_to_op,
                         'basis',          'per_unit',
                         'kind',           'service'),
      v_event_id, p_business_date, p_posted_by, 'op_move'
    );
  END IF;

  PERFORM post_posting_lines(v_batch, FALSE);
  RETURN p_wo_id;
END;
$$;

-- ============================================================
-- 19. post_wo_complete (latest from acct-a41h mig 0101 with yield variance)
-- ============================================================
--
-- - Locks both qty + value pools at last_op (acct-du2.1/.6).
-- - Solo-at-last gate on pre-balance (acct-69e).
-- - cost_method dispatch on parent: standard uses parent_std × qty;
--   wac_* reads pool running avg (already locked).
-- - By-product pre-pass on closing call (gated to standard parent for
--   now; acct-nnyl follow-up):
--     nrv_credit: drain parent WIP at planned_qty × unit_value into
--                 by-product fg; yield variance against by-product fg.
--     negligible: qty leg only; no value, no variance.
--     disposal_cost: qty leg + per-treatment value:
--       inventoriable: per-co-product split into inv_value_fg(co) DR /
--                      accrued_disposal_liability CR (parent WIP UNTOUCHED;
--                      co-products absorb full drain AND get this DR);
--                      yield variance against accrued_disposal_liability.
--       period: disposal_expense_kind DR / accrued_disposal_liability CR.
-- - Co-product distribution loop drains v_total_drain (reduced by
--   v_byproduct_drain).
-- - Residual sweep on FINAL close walks all inv_value_wip(parent, op)
--   and absorbs each non-zero residual via wo_close_v, gated on
--   solo-at-pool stock_wip qty == 0 (acct-69e).

CREATE OR REPLACE FUNCTION post_wo_complete(
  p_wo_id           UUID,
  p_qty             BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id    UUID;
  v_event_id       UUID;
  v_wo             work_orders%ROWTYPE;
  v_last_op        INT;
  v_qty_from       BIGINT;
  v_qty_fg         BIGINT;
  v_val_from       BIGINT;
  v_val_fg         BIGINT;
  v_var_close      BIGINT;
  v_will_close     BOOLEAN;
  v_residual       BIGINT;
  v_batch          JSONB := '[]'::JSONB;
  v_alloc_sum      NUMERIC;
  v_outputs_n      INT;
  v_output         RECORD;
  v_output_idx     INT;
  v_parent_std     BIGINT;
  v_total_drain    BIGINT;
  v_qty_used       BIGINT := 0;
  v_val_used       BIGINT := 0;
  v_q_share        BIGINT;
  v_v_share        BIGINT;
  v_op_residual    RECORD;
  v_pool_at_last   BIGINT;
  v_prebalance     BIGINT;
  v_cost_method    cost_method;
  v_pool_qty       BIGINT;
  v_unit           BIGINT;
  v_pool_qty_pre   BIGINT;
  v_op_qty_acct    BIGINT;
  v_op_qty         BIGINT;
  v_solo_at_last   BOOLEAN;
  v_lock_first     BIGINT;
  v_lock_second    BIGINT;
  v_bp             wo_by_products%ROWTYPE;
  v_bp_qty_acct    BIGINT;
  v_bp_val_acct    BIGINT;
  v_void_qty       BIGINT;
  v_byproduct_drain BIGINT := 0;
  v_disp_total       BIGINT;
  v_disp_liability   BIGINT;
  v_disp_exp_acct    BIGINT;
  v_disp_exp_kind    account_kind;
  v_disp_share       BIGINT;
  v_disp_used        BIGINT;
  v_disp_output      RECORD;
  v_disp_output_idx  INT;
  v_yield_var_acct   BIGINT;
  v_yield_qty_delta  BIGINT;
  v_yield_amount     BIGINT;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: wo_complete qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not released',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  IF v_wo.qty_completed + v_wo.qty_scrapped + p_qty > v_wo.qty_target THEN
    RAISE EXCEPTION
      'wo_qty_overflow: WO % completed=% scrapped=% + this=% > target=%',
      p_wo_id, v_wo.qty_completed, v_wo.qty_scrapped, p_qty, v_wo.qty_target
      USING ERRCODE = 'P0027';
  END IF;

  v_will_close :=
    (v_wo.qty_completed + v_wo.qty_scrapped + p_qty) = v_wo.qty_target;

  SELECT MAX(routing_op) INTO v_last_op FROM wo_routings WHERE wo_id = p_wo_id;
  IF v_last_op IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no routing operations', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_last_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, v_last_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=v_last_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_from IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, v_last_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;

  SELECT COUNT(*), COALESCE(SUM(allocation_pct), 0)
    INTO v_outputs_n, v_alloc_sum
    FROM wo_outputs WHERE wo_id = p_wo_id;
  IF v_outputs_n = 0 THEN
    RAISE EXCEPTION 'wo_invalid: WO % has no wo_outputs rows', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;
  IF v_alloc_sum <> 100 THEN
    RAISE EXCEPTION
      'output_allocation_invalid: wo_outputs(wo=%) allocation_pct sums to % (expected 100)',
      p_wo_id, v_alloc_sum USING ERRCODE = 'P0033';
  END IF;

  v_lock_first  := LEAST(v_qty_from, v_val_from);
  v_lock_second := GREATEST(v_qty_from, v_val_from);
  PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
  PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

  SELECT (debits_total - credits_total) INTO v_pool_qty_pre
    FROM accounts WHERE id = v_qty_from;
  v_solo_at_last := COALESCE(v_pool_qty_pre, 0) = p_qty;

  SELECT cost_method INTO v_cost_method FROM skus WHERE id = v_wo.parent_sku_id;

  IF v_cost_method = 'standard' THEN
    v_parent_std  := _resolve_standard_cost_at(v_wo.parent_sku_id, p_business_date);
    v_total_drain := p_qty * v_parent_std;

  ELSIF v_cost_method IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive') THEN
    SELECT (debits_total - credits_total) INTO v_pool_at_last
      FROM accounts WHERE id = v_val_from;
    SELECT (debits_total - credits_total) INTO v_pool_qty
      FROM accounts WHERE id = v_qty_from;

    IF v_pool_qty IS NULL OR v_pool_qty <= 0 THEN
      v_unit := 0;
    ELSE
      v_unit := GREATEST(COALESCE(v_pool_at_last, 0), 0) / v_pool_qty;
    END IF;
    v_total_drain := p_qty * v_unit;

  ELSE
    RAISE EXCEPTION
      'wo_invalid: parent_sku % has cost_method=% which post_wo_complete does not handle',
      v_wo.parent_sku_id, v_cost_method USING ERRCODE = 'P0026';
  END IF;

  INSERT INTO wo_events (
    wo_id, event_kind, routing_op_from, qty,
    business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'wo_complete', v_last_op, p_qty,
    p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  IF v_will_close AND v_solo_at_last THEN
    IF v_cost_method = 'standard' THEN
      SELECT (debits_total - credits_total) INTO v_pool_at_last
        FROM accounts WHERE id = v_val_from;
    END IF;
    v_prebalance := v_total_drain - COALESCE(v_pool_at_last, 0);

    IF v_prebalance <> 0 THEN
      SELECT id INTO v_var_close FROM accounts
       WHERE kind='variance_wo_close' AND ledger_kind='value'
         AND currency=v_wo.currency AND NOT is_closed;
      IF v_var_close IS NULL THEN
        RAISE EXCEPTION 'no open variance_wo_close account for ccy=%',
                        v_wo.currency USING ERRCODE = 'P0010';
      END IF;

      IF v_prebalance > 0 THEN
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_val_from,
          'credit_account_id', v_var_close,
          'amount',            v_prebalance,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      ELSE
        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_var_close,
          'credit_account_id', v_val_from,
          'amount',            -v_prebalance,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;
    END IF;
  END IF;

  -- By-products pre-pass on closing call. Standard parent only (acct-nnyl
  -- follow-up for WAC-parent + by-product interaction).
  IF v_will_close AND v_cost_method = 'standard' THEN
    SELECT id INTO v_void_qty FROM accounts
     WHERE kind='creation_void' AND ledger_kind='qty' AND NOT is_closed;

    FOR v_bp IN
      SELECT * FROM wo_by_products WHERE wo_id = p_wo_id
       ORDER BY by_product_no
    LOOP
      v_yield_qty_delta := v_bp.actual_qty - v_bp.planned_qty;

      IF v_bp.actual_qty > 0 THEN
        IF v_void_qty IS NULL THEN
          RAISE EXCEPTION 'no creation_void(qty) account configured'
            USING ERRCODE = 'P0010';
        END IF;
        SELECT id INTO v_bp_qty_acct FROM accounts
         WHERE kind='stock_available' AND sku_id=v_bp.output_sku_id
           AND location_id=v_bp.fg_location_id AND NOT is_closed;
        IF v_bp_qty_acct IS NULL THEN
          RAISE EXCEPTION
            'no open stock_available account for by-product sku=% loc=%',
            v_bp.output_sku_id, v_bp.fg_location_id USING ERRCODE = 'P0010';
        END IF;

        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_complete',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_bp_qty_acct,
          'credit_account_id', v_void_qty,
          'amount',            v_bp.actual_qty,
          'qty',               v_bp.actual_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));
      END IF;

      IF v_bp.treatment = 'nrv_credit' THEN
        SELECT id INTO v_bp_val_acct FROM accounts
         WHERE kind='inv_value_fg' AND sku_id=v_bp.output_sku_id
           AND location_id=v_bp.fg_location_id AND currency=v_wo.currency
           AND NOT is_closed;
        IF v_bp_val_acct IS NULL THEN
          RAISE EXCEPTION
            'no open inv_value_fg account for by-product sku=% loc=% ccy=%',
            v_bp.output_sku_id, v_bp.fg_location_id, v_wo.currency
            USING ERRCODE = 'P0010';
        END IF;

        v_byproduct_drain := v_byproduct_drain + v_bp.unit_value * v_bp.planned_qty;

        v_batch := v_batch || jsonb_build_array(jsonb_build_object(
          'reason',            'wo_complete_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_bp_val_acct,
          'credit_account_id', v_val_from,
          'amount',            v_bp.unit_value * v_bp.planned_qty,
          'qty',               v_bp.planned_qty,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        ));

        IF v_yield_qty_delta <> 0 THEN
          SELECT id INTO v_yield_var_acct FROM accounts
           WHERE kind='variance_yield_byproduct' AND ledger_kind='value'
             AND currency=v_wo.currency AND NOT is_closed;
          IF v_yield_var_acct IS NULL THEN
            RAISE EXCEPTION
              'no open variance_yield_byproduct account for ccy=%',
              v_wo.currency USING ERRCODE = 'P0010';
          END IF;

          v_yield_amount := v_yield_qty_delta * v_bp.unit_value;
          IF v_yield_amount > 0 THEN
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_complete_v',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_bp_val_acct,
              'credit_account_id', v_yield_var_acct,
              'amount',            v_yield_amount,
              'qty',               v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          ELSE
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_complete_v',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_yield_var_acct,
              'credit_account_id', v_bp_val_acct,
              'amount',            -v_yield_amount,
              'qty',               -v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          END IF;
        END IF;

      ELSIF v_bp.treatment = 'disposal_cost' THEN
        SELECT id INTO v_disp_liability FROM accounts
         WHERE kind = 'accrued_disposal_liability'
           AND counterparty_id = v_bp.disposal_vendor_id
           AND currency = v_wo.currency
           AND NOT is_closed;
        IF v_disp_liability IS NULL THEN
          RAISE EXCEPTION
            'no open accrued_disposal_liability account for vendor=% ccy=%',
            v_bp.disposal_vendor_id, v_wo.currency
            USING ERRCODE = 'P0010';
        END IF;

        v_disp_total := ABS(v_bp.unit_value) * v_bp.planned_qty;

        IF v_bp.disposal_basis = 'period' THEN
          v_disp_exp_kind := COALESCE(
            v_bp.disposal_expense_account_kind,
            'disposal_expense'::account_kind
          );
          SELECT id INTO v_disp_exp_acct FROM accounts
           WHERE kind = v_disp_exp_kind
             AND ledger_kind = 'value'
             AND currency = v_wo.currency
             AND NOT is_closed;
          IF v_disp_exp_acct IS NULL THEN
            RAISE EXCEPTION
              'no open % account for ccy=%',
              v_disp_exp_kind, v_wo.currency USING ERRCODE = 'P0010';
          END IF;

          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'wo_complete_v',
            'document_kind',     'wo_complete',
            'document_id',       v_event_id,
            'debit_account_id',  v_disp_exp_acct,
            'credit_account_id', v_disp_liability,
            'amount',            v_disp_total,
            'qty',               v_bp.planned_qty,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'posted_by',         p_posted_by
          ));

        ELSIF v_bp.disposal_basis = 'inventoriable' THEN
          v_disp_used := 0;
          v_disp_output_idx := 0;
          FOR v_disp_output IN
            SELECT * FROM wo_outputs WHERE wo_id = p_wo_id
             ORDER BY output_no
          LOOP
            v_disp_output_idx := v_disp_output_idx + 1;
            IF v_disp_output_idx = v_outputs_n THEN
              v_disp_share := v_disp_total - v_disp_used;
            ELSE
              v_disp_share := (v_disp_total * v_disp_output.allocation_pct)::BIGINT / 100;
            END IF;
            v_disp_used := v_disp_used + v_disp_share;

            IF v_disp_share = 0 THEN
              CONTINUE;
            END IF;

            SELECT id INTO v_val_fg FROM accounts
             WHERE kind = 'inv_value_fg'
               AND sku_id = v_disp_output.output_sku_id
               AND location_id = v_disp_output.fg_location_id
               AND currency = v_wo.currency
               AND NOT is_closed;
            IF v_val_fg IS NULL THEN
              RAISE EXCEPTION
                'no open inv_value_fg account for sku=% loc=% ccy=%',
                v_disp_output.output_sku_id, v_disp_output.fg_location_id, v_wo.currency
                USING ERRCODE = 'P0010';
            END IF;

            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_complete_v',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_val_fg,
              'credit_account_id', v_disp_liability,
              'amount',            v_disp_share,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          END LOOP;
        END IF;

        IF v_yield_qty_delta <> 0 THEN
          SELECT id INTO v_yield_var_acct FROM accounts
           WHERE kind='variance_yield_byproduct' AND ledger_kind='value'
             AND currency=v_wo.currency AND NOT is_closed;
          IF v_yield_var_acct IS NULL THEN
            RAISE EXCEPTION
              'no open variance_yield_byproduct account for ccy=%',
              v_wo.currency USING ERRCODE = 'P0010';
          END IF;

          v_yield_amount := v_yield_qty_delta * ABS(v_bp.unit_value);
          IF v_yield_amount > 0 THEN
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_complete_v',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_yield_var_acct,
              'credit_account_id', v_disp_liability,
              'amount',            v_yield_amount,
              'qty',               v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          ELSE
            v_batch := v_batch || jsonb_build_array(jsonb_build_object(
              'reason',            'wo_complete_v',
              'document_kind',     'wo_complete',
              'document_id',       v_event_id,
              'debit_account_id',  v_disp_liability,
              'credit_account_id', v_yield_var_acct,
              'amount',            -v_yield_amount,
              'qty',               -v_yield_qty_delta,
              'business_date',     p_business_date,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         p_posted_by
            ));
          END IF;
        END IF;
      END IF;
      -- negligible: qty leg only (already emitted above when actual_qty > 0).
    END LOOP;

    v_total_drain := v_total_drain - v_byproduct_drain;
  END IF;

  -- Co-product distribution loop.
  v_output_idx := 0;
  FOR v_output IN
    SELECT * FROM wo_outputs WHERE wo_id = p_wo_id
     ORDER BY output_no
  LOOP
    v_output_idx := v_output_idx + 1;
    IF v_output_idx = v_outputs_n THEN
      v_q_share := p_qty - v_qty_used;
    ELSE
      v_q_share := (v_output.qty * p_qty) / v_wo.qty_target;
    END IF;
    v_qty_used := v_qty_used + v_q_share;

    IF v_output_idx = v_outputs_n THEN
      v_v_share := v_total_drain - v_val_used;
    ELSE
      v_v_share := (v_total_drain * v_output.allocation_pct)::BIGINT / 100;
    END IF;
    v_val_used := v_val_used + v_v_share;

    SELECT id INTO v_qty_fg FROM accounts
     WHERE kind='stock_available' AND sku_id=v_output.output_sku_id
       AND location_id=v_output.fg_location_id AND NOT is_closed;
    IF v_qty_fg IS NULL THEN
      RAISE EXCEPTION 'no open stock_available account for sku=% loc=%',
                      v_output.output_sku_id, v_output.fg_location_id
        USING ERRCODE = 'P0010';
    END IF;
    SELECT id INTO v_val_fg FROM accounts
     WHERE kind='inv_value_fg' AND sku_id=v_output.output_sku_id
       AND location_id=v_output.fg_location_id AND currency=v_wo.currency
       AND NOT is_closed;
    IF v_val_fg IS NULL THEN
      RAISE EXCEPTION 'no open inv_value_fg account for sku=% loc=% ccy=%',
                      v_output.output_sku_id, v_output.fg_location_id, v_wo.currency
        USING ERRCODE = 'P0010';
    END IF;

    IF v_q_share > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'wo_complete',
        'document_kind',     'wo_complete',
        'document_id',       v_event_id,
        'debit_account_id',  v_qty_fg,
        'credit_account_id', v_qty_from,
        'amount',            v_q_share,
        'qty',               v_q_share,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));
    END IF;

    IF v_v_share > 0 THEN
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'wo_complete_v',
        'document_kind',     'wo_complete',
        'document_id',       v_event_id,
        'debit_account_id',  v_val_fg,
        'credit_account_id', v_val_from,
        'amount',            v_v_share,
        'qty',               v_q_share,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      ));
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);
  UPDATE work_orders SET qty_completed = qty_completed + p_qty
   WHERE id = p_wo_id;

  -- Residual sweep (FINAL close only). solo-at-pool gate per acct-69e.
  IF v_will_close THEN
    FOR v_op_residual IN
      SELECT a.id AS acct_id,
             a.routing_op AS rop,
             (a.debits_total - a.credits_total) AS balance
        FROM accounts a
       WHERE a.kind = 'inv_value_wip'
         AND a.sku_id = v_wo.parent_sku_id
         AND a.currency = v_wo.currency
         AND a.routing_op IN (
           SELECT routing_op FROM wo_routings WHERE wo_id = p_wo_id
         )
         AND NOT a.is_closed
       ORDER BY a.routing_op
    LOOP
      SELECT id INTO v_op_qty_acct FROM accounts
       WHERE kind = 'stock_wip' AND sku_id = v_wo.parent_sku_id
         AND routing_op = v_op_residual.rop AND NOT is_closed;
      IF v_op_qty_acct IS NULL THEN
        v_op_qty := 0;
      ELSE
        v_lock_first  := LEAST(v_op_qty_acct, v_op_residual.acct_id);
        v_lock_second := GREATEST(v_op_qty_acct, v_op_residual.acct_id);
        PERFORM 1 FROM accounts WHERE id = v_lock_first  FOR UPDATE;
        PERFORM 1 FROM accounts WHERE id = v_lock_second FOR UPDATE;

        SELECT (debits_total - credits_total) INTO v_op_qty
          FROM accounts WHERE id = v_op_qty_acct;
      END IF;
      IF COALESCE(v_op_qty, 0) <> 0 THEN
        CONTINUE;
      END IF;

      SELECT (debits_total - credits_total) INTO v_residual
        FROM accounts WHERE id = v_op_residual.acct_id;
      IF v_residual = 0 OR v_residual IS NULL THEN CONTINUE; END IF;

      SELECT id INTO v_var_close FROM accounts
       WHERE kind='variance_wo_close' AND ledger_kind='value'
         AND currency=v_wo.currency AND NOT is_closed;
      IF v_var_close IS NULL THEN
        RAISE EXCEPTION 'no open variance_wo_close account for ccy=%',
                        v_wo.currency USING ERRCODE = 'P0010';
      END IF;

      IF v_residual > 0 THEN
        PERFORM post_posting_lines(jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_var_close,
          'credit_account_id', v_op_residual.acct_id,
          'amount',            v_residual,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )), FALSE);
      ELSE
        PERFORM post_posting_lines(jsonb_build_array(jsonb_build_object(
          'reason',            'wo_close_v',
          'document_kind',     'wo_complete',
          'document_id',       v_event_id,
          'debit_account_id',  v_op_residual.acct_id,
          'credit_account_id', v_var_close,
          'amount',            -v_residual,
          'business_date',     p_business_date,
          'idempotency_key',   gen_random_uuid(),
          'posted_by',         p_posted_by
        )), FALSE);
      END IF;
    END LOOP;

    UPDATE work_orders SET status = 'closed' WHERE id = p_wo_id;
  END IF;

  RETURN p_wo_id;
END;
$$;

-- ============================================================
-- 20. post_scrap (acct-du2.5 dual-check + lock pair)
-- ============================================================

CREATE OR REPLACE FUNCTION post_scrap(
  p_wo_id           UUID,
  p_routing_op      INT,
  p_qty             BIGINT,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id    UUID;
  v_event_id       UUID;
  v_wo             work_orders%ROWTYPE;
  v_op_count       INT;
  v_qty_from       BIGINT;
  v_qty_scrap      BIGINT;
  v_val_from       BIGINT;
  v_var_scrap      BIGINT;
  v_qty_balance    BIGINT;
  v_val_balance    BIGINT;
  v_unit_cost      BIGINT;
  v_scrap_value    BIGINT;
  v_batch          JSONB := '[]'::JSONB;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: scrap qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not released',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  IF v_wo.qty_completed + v_wo.qty_scrapped + p_qty > v_wo.qty_target THEN
    RAISE EXCEPTION
      'wo_qty_overflow: WO % completed=% scrapped=% + this=% > target=%',
      p_wo_id, v_wo.qty_completed, v_wo.qty_scrapped, p_qty, v_wo.qty_target
      USING ERRCODE = 'P0027';
  END IF;

  SELECT COUNT(*) INTO v_op_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_routing_op;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: op % not in WO % routing',
                    p_routing_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_routing_op USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_qty_scrap FROM accounts
   WHERE kind='stock_scrap' AND sku_id=v_wo.parent_sku_id
     AND ledger_kind='qty' AND NOT is_closed;
  IF v_qty_scrap IS NULL THEN
    RAISE EXCEPTION 'no open stock_scrap account for sku=%',
                    v_wo.parent_sku_id USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_val_from FROM accounts
   WHERE kind='inv_value_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND currency=v_wo.currency AND NOT is_closed;
  IF v_val_from IS NULL THEN
    RAISE EXCEPTION 'no open inv_value_wip account for sku=% op=% ccy=%',
                    v_wo.parent_sku_id, p_routing_op, v_wo.currency
      USING ERRCODE = 'P0010';
  END IF;
  SELECT id INTO v_var_scrap FROM accounts
   WHERE kind='variance_scrap' AND ledger_kind='value'
     AND currency=v_wo.currency AND NOT is_closed;
  IF v_var_scrap IS NULL THEN
    RAISE EXCEPTION 'no open variance_scrap account for ccy=%',
                    v_wo.currency USING ERRCODE = 'P0010';
  END IF;

  PERFORM 1 FROM accounts WHERE id IN (v_val_from, v_qty_from)
   ORDER BY id FOR UPDATE;
  SELECT (debits_total - credits_total) INTO v_qty_balance
    FROM accounts WHERE id = v_qty_from;
  SELECT (debits_total - credits_total) INTO v_val_balance
    FROM accounts WHERE id = v_val_from;

  IF v_qty_balance IS NULL OR v_qty_balance <= 0 THEN
    RAISE EXCEPTION
      'wo_invalid: stock_wip(sku=%, op=%) balance=%, cannot scrap',
      v_wo.parent_sku_id, p_routing_op, v_qty_balance USING ERRCODE = 'P0026';
  END IF;
  IF p_qty > v_qty_balance THEN
    RAISE EXCEPTION
      'wo_invalid: scrap qty=% > stock_wip balance=% at op=%',
      p_qty, v_qty_balance, p_routing_op USING ERRCODE = 'P0026';
  END IF;
  v_unit_cost   := COALESCE(v_val_balance, 0) / v_qty_balance;
  v_scrap_value := v_unit_cost * p_qty;

  INSERT INTO wo_events (
    wo_id, event_kind, routing_op_from, qty,
    business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'scrap', p_routing_op, p_qty,
    p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  v_batch := v_batch || jsonb_build_array(jsonb_build_object(
    'reason',            'scrap',
    'document_kind',     'scrap',
    'document_id',       v_event_id,
    'debit_account_id',  v_qty_scrap,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   gen_random_uuid(),
    'posted_by',         p_posted_by
  ));

  IF v_scrap_value > 0 THEN
    v_batch := v_batch || jsonb_build_array(jsonb_build_object(
      'reason',            'scrap_v',
      'document_kind',     'scrap',
      'document_id',       v_event_id,
      'debit_account_id',  v_var_scrap,
      'credit_account_id', v_val_from,
      'amount',            v_scrap_value,
      'qty',               p_qty,
      'business_date',     p_business_date,
      'idempotency_key',   gen_random_uuid(),
      'posted_by',         p_posted_by
    ));
  END IF;

  PERFORM post_posting_lines(v_batch, FALSE);
  UPDATE work_orders SET qty_scrapped = qty_scrapped + p_qty WHERE id = p_wo_id;
  RETURN p_wo_id;
END;
$$;

-- ============================================================
-- 21. post_wo_close_unproduced (acct-du2.10 solo-at-pool gate)
-- ============================================================

CREATE OR REPLACE FUNCTION post_wo_close_unproduced(
  p_wo_id           UUID,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id  UUID;
  v_event_id     UUID;
  v_wo           work_orders%ROWTYPE;
  v_var_close    BIGINT;
  v_residual     BIGINT;
  v_op_residual  RECORD;
  v_pool_qty     BIGINT;
BEGIN
  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id
      USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_existing_id FROM wo_events
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION
      'wo_close_unproduced_invalid_state: WO % status=% not released',
      p_wo_id, v_wo.status USING ERRCODE = 'P0034';
  END IF;
  IF v_wo.qty_completed <> 0 THEN
    RAISE EXCEPTION
      'wo_close_unproduced_invalid_state: WO % qty_completed=% (must be 0; '
      'use post_wo_complete if any units finished)',
      p_wo_id, v_wo.qty_completed USING ERRCODE = 'P0034';
  END IF;
  IF v_wo.qty_scrapped <> v_wo.qty_target THEN
    RAISE EXCEPTION
      'wo_close_unproduced_invalid_state: WO % qty_scrapped=% qty_target=% '
      '(must scrap full target before unproduced close)',
      p_wo_id, v_wo.qty_scrapped, v_wo.qty_target USING ERRCODE = 'P0034';
  END IF;

  INSERT INTO wo_events (
    wo_id, event_kind, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_wo_id, 'close_unproduced', p_business_date, p_posted_by, p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_event_id;
  IF v_event_id IS NULL THEN RETURN p_wo_id; END IF;

  FOR v_op_residual IN
    SELECT a.id AS acct_id, a.routing_op
      FROM accounts a
     WHERE a.kind = 'inv_value_wip'
       AND a.sku_id = v_wo.parent_sku_id
       AND a.currency = v_wo.currency
       AND a.routing_op IN (
         SELECT routing_op FROM wo_routings WHERE wo_id = p_wo_id
       )
       AND NOT a.is_closed
     ORDER BY a.routing_op
  LOOP
    SELECT (debits_total - credits_total) INTO v_pool_qty
      FROM accounts
     WHERE kind = 'stock_wip' AND sku_id = v_wo.parent_sku_id
       AND routing_op = v_op_residual.routing_op AND NOT is_closed;
    IF COALESCE(v_pool_qty, 0) <> 0 THEN
      CONTINUE;
    END IF;

    PERFORM 1 FROM accounts WHERE id = v_op_residual.acct_id FOR UPDATE;
    SELECT (debits_total - credits_total) INTO v_residual
      FROM accounts WHERE id = v_op_residual.acct_id;
    IF v_residual = 0 OR v_residual IS NULL THEN CONTINUE; END IF;

    SELECT id INTO v_var_close FROM accounts
     WHERE kind='variance_wo_close' AND ledger_kind='value'
       AND currency=v_wo.currency AND NOT is_closed;
    IF v_var_close IS NULL THEN
      RAISE EXCEPTION 'no open variance_wo_close account for ccy=%',
                      v_wo.currency USING ERRCODE = 'P0010';
    END IF;

    IF v_residual > 0 THEN
      PERFORM post_posting_lines(jsonb_build_array(jsonb_build_object(
        'reason',            'wo_close_v',
        'document_kind',     'wo_close_unproduced',
        'document_id',       v_event_id,
        'debit_account_id',  v_var_close,
        'credit_account_id', v_op_residual.acct_id,
        'amount',            v_residual,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      )), FALSE);
    ELSE
      PERFORM post_posting_lines(jsonb_build_array(jsonb_build_object(
        'reason',            'wo_close_v',
        'document_kind',     'wo_close_unproduced',
        'document_id',       v_event_id,
        'debit_account_id',  v_op_residual.acct_id,
        'credit_account_id', v_var_close,
        'amount',            -v_residual,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'posted_by',         p_posted_by
      )), FALSE);
    END IF;
  END LOOP;

  UPDATE work_orders SET status = 'closed' WHERE id = p_wo_id;
  RETURN p_wo_id;
END;
$$;

-- ============================================================
-- 22. post_osp_ship / post_osp_receive (acct-du2.3 dual-check)
-- ============================================================
--
-- Outside processing custody: parent units leave plant → vendor (qty
-- only; value stays in inv_value_wip(parent, op)). Service charge for
-- the OSP step is a bom_line of kind='service' fired at op_arrival via
-- _wo_emit_bom_lines on first arrival at the OSP op. These functions
-- only move qty.

CREATE OR REPLACE FUNCTION post_osp_ship(
  p_wo_id           UUID,
  p_routing_op      INT,
  p_qty             BIGINT,
  p_vendor_id       UUID,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing      BIGINT;
  v_wo            work_orders%ROWTYPE;
  v_op_count      INT;
  v_qty_from      BIGINT;
  v_qty_to        BIGINT;
  v_wip_balance   BIGINT;
BEGIN
  SELECT id INTO v_existing FROM posting_lines
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: osp_ship qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;
  IF p_vendor_id IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: osp_ship requires vendor_id'
      USING ERRCODE = 'P0026';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_existing FROM posting_lines
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not released',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT COUNT(*) INTO v_op_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_routing_op;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: op % not in WO % routing',
                    p_routing_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_routing_op USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_qty_to FROM accounts
   WHERE kind='stock_consigned'
     AND sku_id=v_wo.parent_sku_id
     AND counterparty_id=p_vendor_id
     AND routing_op=p_routing_op
     AND NOT is_closed;
  IF v_qty_to IS NULL THEN
    RAISE EXCEPTION 'no open stock_consigned account for sku=% vendor=% op=%',
                    v_wo.parent_sku_id, p_vendor_id, p_routing_op
      USING ERRCODE = 'P0010';
  END IF;

  PERFORM 1 FROM accounts WHERE id IN (v_qty_from, v_qty_to)
   ORDER BY id FOR UPDATE;
  SELECT (debits_total - credits_total) INTO v_wip_balance
    FROM accounts WHERE id = v_qty_from;
  IF p_qty > v_wip_balance THEN
    RAISE EXCEPTION
      'wo_invalid: osp_ship qty=% > stock_wip balance=% at sku=% op=%',
      p_qty, v_wip_balance, v_wo.parent_sku_id, p_routing_op
      USING ERRCODE = 'P0026';
  END IF;

  PERFORM post_posting_lines(jsonb_build_array(jsonb_build_object(
    'reason',            'osp_ship',
    'document_kind',     'osp_ship',
    'document_id',       p_wo_id,
    'debit_account_id',  v_qty_to,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   p_idempotency_key,
    'posted_by',         p_posted_by,
    'counterparty_id',   p_vendor_id,
    'notes',             p_notes
  )), FALSE);

  RETURN p_wo_id;
END;
$$;

CREATE OR REPLACE FUNCTION post_osp_receive(
  p_wo_id           UUID,
  p_routing_op      INT,
  p_qty             BIGINT,
  p_vendor_id       UUID,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing      BIGINT;
  v_wo            work_orders%ROWTYPE;
  v_op_count      INT;
  v_qty_from      BIGINT;
  v_qty_to        BIGINT;
  v_consigned     BIGINT;
BEGIN
  SELECT id INTO v_existing FROM posting_lines
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF p_qty IS NULL OR p_qty <= 0 THEN
    RAISE EXCEPTION 'wo_invalid: osp_receive qty must be > 0 (got %)', p_qty
      USING ERRCODE = 'P0026';
  END IF;
  IF p_vendor_id IS NULL THEN
    RAISE EXCEPTION 'wo_invalid: osp_receive requires vendor_id'
      USING ERRCODE = 'P0026';
  END IF;

  SELECT * INTO v_wo FROM work_orders WHERE id = p_wo_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'wo_invalid: WO % not found', p_wo_id USING ERRCODE = 'P0026';
  END IF;

  SELECT id INTO v_existing FROM posting_lines
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing IS NOT NULL THEN RETURN p_wo_id; END IF;

  IF v_wo.status <> 'released' THEN
    RAISE EXCEPTION 'wo_invalid: WO % status=% not released',
                    p_wo_id, v_wo.status USING ERRCODE = 'P0026';
  END IF;

  SELECT COUNT(*) INTO v_op_count FROM wo_routings
   WHERE wo_id = p_wo_id AND routing_op = p_routing_op;
  IF v_op_count = 0 THEN
    RAISE EXCEPTION 'routing_op_invalid: op % not in WO % routing',
                    p_routing_op, p_wo_id USING ERRCODE = 'P0028';
  END IF;

  SELECT id INTO v_qty_from FROM accounts
   WHERE kind='stock_consigned'
     AND sku_id=v_wo.parent_sku_id
     AND counterparty_id=p_vendor_id
     AND routing_op=p_routing_op
     AND NOT is_closed;
  IF v_qty_from IS NULL THEN
    RAISE EXCEPTION 'no open stock_consigned account for sku=% vendor=% op=%',
                    v_wo.parent_sku_id, p_vendor_id, p_routing_op
      USING ERRCODE = 'P0010';
  END IF;

  SELECT id INTO v_qty_to FROM accounts
   WHERE kind='stock_wip' AND sku_id=v_wo.parent_sku_id
     AND routing_op=p_routing_op AND NOT is_closed;
  IF v_qty_to IS NULL THEN
    RAISE EXCEPTION 'no open stock_wip account for sku=% op=%',
                    v_wo.parent_sku_id, p_routing_op USING ERRCODE = 'P0010';
  END IF;

  PERFORM 1 FROM accounts WHERE id IN (v_qty_from, v_qty_to)
   ORDER BY id FOR UPDATE;
  SELECT (debits_total - credits_total) INTO v_consigned
    FROM accounts WHERE id = v_qty_from;
  IF p_qty > COALESCE(v_consigned, 0) THEN
    RAISE EXCEPTION
      'osp_yield_overflow: osp_receive qty=% > stock_consigned balance=% '
      '(sku=% vendor=% op=%); cannot receive more than was shipped',
      p_qty, v_consigned, v_wo.parent_sku_id, p_vendor_id, p_routing_op
      USING ERRCODE = 'P0030';
  END IF;

  PERFORM post_posting_lines(jsonb_build_array(jsonb_build_object(
    'reason',            'osp_receive',
    'document_kind',     'osp_receive',
    'document_id',       p_wo_id,
    'debit_account_id',  v_qty_to,
    'credit_account_id', v_qty_from,
    'amount',            p_qty,
    'qty',               p_qty,
    'business_date',     p_business_date,
    'idempotency_key',   p_idempotency_key,
    'posted_by',         p_posted_by,
    'counterparty_id',   p_vendor_id,
    'notes',             p_notes
  )), FALSE);

  RETURN p_wo_id;
END;
$$;

-- ============================================================
-- 23. ALTER vendor_bill_lines + post_ap_bill replacement (disposal_match)
-- ============================================================

ALTER TABLE vendor_bill_lines
  ADD COLUMN IF NOT EXISTS disposal_wo_event_id UUID REFERENCES wo_events(id),
  ADD COLUMN IF NOT EXISTS by_product_no        INT;

-- IF EXISTS guards the revert+reapply path: the down drops these
-- constraints, so on reapply we must tolerate their absence.
ALTER TABLE vendor_bill_lines DROP CONSTRAINT IF EXISTS vendor_bill_lines_check;
ALTER TABLE vendor_bill_lines DROP CONSTRAINT IF EXISTS vendor_bill_lines_kind_check;

ALTER TABLE vendor_bill_lines
  ADD CONSTRAINT vendor_bill_lines_kind_check
  CHECK (kind IN ('po_match', 'service', 'disposal_match'));

ALTER TABLE vendor_bill_lines
  ADD CONSTRAINT vendor_bill_lines_check
  CHECK (
    (kind = 'po_match'
     AND po_line_id IS NOT NULL
     AND expense_account_id IS NULL
     AND qty IS NOT NULL AND qty > 0
     AND unit_cost IS NOT NULL AND unit_cost >= 0
     AND disposal_wo_event_id IS NULL
     AND by_product_no IS NULL)
    OR
    (kind = 'service'
     AND po_line_id IS NULL
     AND expense_account_id IS NOT NULL
     AND qty IS NULL
     AND unit_cost IS NULL
     AND disposal_wo_event_id IS NULL
     AND by_product_no IS NULL)
    OR
    (kind = 'disposal_match'
     AND po_line_id IS NULL
     AND expense_account_id IS NULL
     AND qty IS NOT NULL AND qty > 0
     AND unit_cost IS NOT NULL AND unit_cost >= 0
     AND disposal_wo_event_id IS NOT NULL
     AND by_product_no IS NOT NULL)
  );

CREATE INDEX vendor_bill_lines_disposal_event
  ON vendor_bill_lines (disposal_wo_event_id, by_product_no)
  WHERE disposal_wo_event_id IS NOT NULL;

CREATE OR REPLACE FUNCTION post_ap_bill(
  p_vendor_id       UUID,
  p_currency        CHAR(3),
  p_lines           JSONB,
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
  v_existing_id      UUID;
  v_doc_id           UUID;
  v_vendor_check     UUID;
  v_tolerance_pct    NUMERIC(5,2);
  v_n                INT;
  v_idx              INT;
  v_line             JSONB;
  v_kind             TEXT;
  v_po_line_id       UUID;
  v_qty              BIGINT;
  v_unit_cost        BIGINT;
  v_amount           BIGINT;
  v_expense_acct     BIGINT;
  v_pl               RECORD;
  v_total_received   BIGINT;
  v_total_billed     BIGINT;
  v_returns_to_us    BIGINT;
  v_avail            BIGINT;
  v_ven_unsettled    BIGINT;
  v_ven_ap           BIGINT;
  v_match_tol_acct   BIGINT;
  v_exp_acct         accounts%ROWTYPE;
  v_bill_line_id     UUID;
  v_diff_total       BIGINT;
  v_diff_pct         NUMERIC(10,4);
  v_amount_at_po     BIGINT;
  v_batch            JSONB := '[]'::JSONB;
  v_disp_event_id    UUID;
  v_by_product_no    INT;
  v_disp_wo_id       UUID;
  v_wo_currency      CHAR(3);
  v_bp               wo_by_products%ROWTYPE;
  v_accrued_unit     BIGINT;
  v_disp_liability   BIGINT;
  v_amount_at_accrual BIGINT;
BEGIN
  SELECT id INTO v_existing_id FROM vendor_bills
   WHERE idempotency_key = p_idempotency_key;
  IF v_existing_id IS NOT NULL THEN RETURN v_existing_id; END IF;

  SELECT id, unit_cost_tolerance_pct INTO v_vendor_check, v_tolerance_pct
    FROM vendors WHERE id = p_vendor_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ap_bill_invalid_line: vendor % not found', p_vendor_id
      USING ERRCODE = 'P0025';
  END IF;

  v_n := jsonb_array_length(p_lines);
  IF v_n = 0 THEN
    RAISE EXCEPTION 'ap_bill_invalid_line: empty bill for vendor %', p_vendor_id
      USING ERRCODE = 'P0025';
  END IF;

  SELECT id INTO v_ven_ap FROM accounts
   WHERE kind='ap' AND counterparty_id=p_vendor_id
     AND currency=p_currency AND NOT is_closed;
  IF v_ven_ap IS NULL THEN
    RAISE EXCEPTION 'no open ap account for vendor=% ccy=%',
                    p_vendor_id, p_currency USING ERRCODE = 'P0010';
  END IF;

  INSERT INTO vendor_bills (
    vendor_id, currency, business_date, posted_by, idempotency_key, notes
  ) VALUES (
    p_vendor_id, p_currency, p_business_date, p_posted_by,
    p_idempotency_key, p_notes
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id INTO v_doc_id;

  IF v_doc_id IS NULL THEN
    SELECT id INTO v_doc_id FROM vendor_bills WHERE idempotency_key = p_idempotency_key;
    RETURN v_doc_id;
  END IF;

  FOR v_idx IN 1..v_n LOOP
    v_line   := p_lines -> (v_idx - 1);
    v_kind   := v_line->>'kind';
    v_amount := (v_line->>'amount')::BIGINT;

    IF v_kind = 'po_match' THEN
      v_po_line_id := (v_line->>'po_line_id')::UUID;
      v_qty        := (v_line->>'qty')::BIGINT;
      v_unit_cost  := (v_line->>'unit_cost')::BIGINT;

      SELECT pl.po_id, pl.unit_cost, pl.currency, po.vendor_id
        INTO v_pl
        FROM purchase_order_lines pl
        JOIN purchase_orders po ON po.id = pl.po_id
       WHERE pl.id = v_po_line_id
         FOR UPDATE OF pl;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % po_line % not found',
                        v_idx, v_po_line_id USING ERRCODE = 'P0025';
      END IF;
      IF v_pl.vendor_id IS DISTINCT FROM p_vendor_id THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % po_line % belongs to vendor % '
          'but bill is for vendor %',
          v_idx, v_po_line_id, v_pl.vendor_id, p_vendor_id
          USING ERRCODE = 'P0025';
      END IF;
      IF v_pl.currency <> p_currency THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % po_line currency=% but bill currency=%',
          v_idx, v_pl.currency, p_currency USING ERRCODE = 'P0025';
      END IF;

      IF v_unit_cost <> v_pl.unit_cost THEN
        IF v_tolerance_pct = 0 THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % unit_cost % does not match '
            'po_line.unit_cost %',
            v_idx, v_unit_cost, v_pl.unit_cost
            USING ERRCODE = 'P0024';
        END IF;
        IF v_pl.unit_cost = 0 THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % po_line.unit_cost is 0 '
            'but bill unit_cost is % (zero-baseline; out of tolerance, '
            'vendor tolerance %%%)',
            v_idx, v_unit_cost, v_tolerance_pct
            USING ERRCODE = 'P0024';
        END IF;
        v_diff_pct := ABS(v_unit_cost - v_pl.unit_cost) * 100.0 / v_pl.unit_cost;
        IF v_diff_pct > v_tolerance_pct THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % unit_cost % differs from '
            'po_line.unit_cost % by %%% (vendor tolerance %%%)',
            v_idx, v_unit_cost, v_pl.unit_cost, v_diff_pct, v_tolerance_pct
            USING ERRCODE = 'P0024';
        END IF;
      END IF;
      IF v_amount <> v_qty * v_unit_cost THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % amount % <> qty % × unit_cost %',
          v_idx, v_amount, v_qty, v_unit_cost
          USING ERRCODE = 'P0024';
      END IF;

      SELECT COALESCE(SUM(qty_received), 0) INTO v_total_received
        FROM po_receipt_lines WHERE po_line_id = v_po_line_id;
      SELECT COALESCE(SUM(qty), 0) INTO v_total_billed
        FROM vendor_bill_lines
       WHERE po_line_id = v_po_line_id AND kind = 'po_match';
      SELECT COALESCE(SUM(prl.qty_to_ap_unsettled), 0) INTO v_returns_to_us
        FROM po_return_lines prl
        JOIN po_receipt_lines rcl ON rcl.id = prl.recv_line_id
       WHERE rcl.po_line_id = v_po_line_id;
      v_avail := v_total_received - v_total_billed - v_returns_to_us;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % qty % exceeds received-not-'
          'billed-not-returned remainder % for po_line % (received=%, '
          'already billed=%, prior returns to ap_unsettled=%)',
          v_idx, v_qty, v_avail, v_po_line_id, v_total_received,
          v_total_billed, v_returns_to_us
          USING ERRCODE = 'P0024';
      END IF;

      SELECT id INTO v_ven_unsettled FROM accounts
       WHERE kind='ap_unsettled' AND counterparty_id=p_vendor_id
         AND currency=p_currency AND NOT is_closed;
      IF v_ven_unsettled IS NULL THEN
        RAISE EXCEPTION 'no open ap_unsettled account for vendor=% ccy=%',
                        p_vendor_id, p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO vendor_bill_lines (
        bill_id, line_no, kind, po_line_id, qty, unit_cost, amount
      ) VALUES (
        v_doc_id, v_idx, 'po_match', v_po_line_id, v_qty, v_unit_cost, v_amount
      ) RETURNING id INTO v_bill_line_id;

      v_amount_at_po := v_qty * v_pl.unit_cost;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_bill',
        'document_kind',     'vendor_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_bill_line_id,
        'debit_account_id',  v_ven_unsettled,
        'credit_account_id', v_ven_ap,
        'amount',            v_amount_at_po,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));

      v_diff_total := v_amount - v_amount_at_po;
      IF v_diff_total <> 0 THEN
        SELECT id INTO v_match_tol_acct FROM accounts
         WHERE kind='variance_match_tolerance' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_match_tol_acct IS NULL THEN
          RAISE EXCEPTION 'no open variance_match_tolerance account for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;

        IF v_diff_total > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ap_bill',
            'document_kind',     'vendor_bill',
            'document_id',       v_doc_id,
            'document_line_id',  v_bill_line_id,
            'debit_account_id',  v_match_tol_acct,
            'credit_account_id', v_ven_ap,
            'amount',            v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        ELSE
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ap_bill',
            'document_kind',     'vendor_bill',
            'document_id',       v_doc_id,
            'document_line_id',  v_bill_line_id,
            'debit_account_id',  v_ven_ap,
            'credit_account_id', v_match_tol_acct,
            'amount',            -v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        END IF;
      END IF;

    ELSIF v_kind = 'service' THEN
      v_expense_acct := (v_line->>'expense_account_id')::BIGINT;

      SELECT * INTO v_exp_acct FROM accounts WHERE id = v_expense_acct;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense_account_id % not found',
                        v_idx, v_expense_acct USING ERRCODE = 'P0025';
      END IF;
      IF v_exp_acct.is_closed THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense account % is closed',
                        v_idx, v_expense_acct USING ERRCODE = 'P0025';
      END IF;
      IF v_exp_acct.ledger_kind <> 'value' THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense account % is %, expected value',
                        v_idx, v_expense_acct, v_exp_acct.ledger_kind
          USING ERRCODE = 'P0025';
      END IF;
      IF v_exp_acct.currency <> p_currency THEN
        RAISE EXCEPTION 'ap_bill_invalid_line: line % expense account ccy=% but bill ccy=%',
                        v_idx, v_exp_acct.currency, p_currency
          USING ERRCODE = 'P0025';
      END IF;

      INSERT INTO vendor_bill_lines (
        bill_id, line_no, kind, expense_account_id, amount
      ) VALUES (
        v_doc_id, v_idx, 'service', v_expense_acct, v_amount
      ) RETURNING id INTO v_bill_line_id;

      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_bill',
        'document_kind',     'vendor_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_bill_line_id,
        'debit_account_id',  v_expense_acct,
        'credit_account_id', v_ven_ap,
        'amount',            v_amount,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));

    ELSIF v_kind = 'disposal_match' THEN
      v_disp_event_id := (v_line->>'disposal_wo_event_id')::UUID;
      v_by_product_no := (v_line->>'by_product_no')::INT;
      v_qty           := (v_line->>'qty')::BIGINT;
      v_unit_cost     := (v_line->>'unit_cost')::BIGINT;

      SELECT we.wo_id, wo.currency
        INTO v_disp_wo_id, v_wo_currency
        FROM wo_events we
        JOIN work_orders wo ON wo.id = we.wo_id
       WHERE we.id = v_disp_event_id;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % disposal_wo_event_id % not found',
          v_idx, v_disp_event_id USING ERRCODE = 'P0025';
      END IF;
      IF v_wo_currency <> p_currency THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % wo currency=% but bill currency=%',
          v_idx, v_wo_currency, p_currency USING ERRCODE = 'P0025';
      END IF;

      SELECT * INTO v_bp FROM wo_by_products
       WHERE wo_id = v_disp_wo_id AND by_product_no = v_by_product_no
         FOR UPDATE;
      IF NOT FOUND THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % wo_by_products(wo=%,no=%) not found',
          v_idx, v_disp_wo_id, v_by_product_no USING ERRCODE = 'P0025';
      END IF;
      IF v_bp.treatment <> 'disposal_cost' THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % wo_by_products row treatment=% '
          '(only disposal_cost rows accept disposal_match bills)',
          v_idx, v_bp.treatment USING ERRCODE = 'P0025';
      END IF;
      IF v_bp.disposal_vendor_id IS DISTINCT FROM p_vendor_id THEN
        RAISE EXCEPTION
          'ap_bill_invalid_line: line % wo_by_products vendor=% but bill vendor=%',
          v_idx, v_bp.disposal_vendor_id, p_vendor_id
          USING ERRCODE = 'P0025';
      END IF;

      v_accrued_unit := ABS(v_bp.unit_value);

      IF v_unit_cost <> v_accrued_unit THEN
        IF v_tolerance_pct = 0 THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % unit_cost % does not match '
            'accrued unit_value %',
            v_idx, v_unit_cost, v_accrued_unit
            USING ERRCODE = 'P0024';
        END IF;
        IF v_accrued_unit = 0 THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % accrued unit_value is 0 '
            'but bill unit_cost is % (vendor tolerance %%%)',
            v_idx, v_unit_cost, v_tolerance_pct
            USING ERRCODE = 'P0024';
        END IF;
        v_diff_pct := ABS(v_unit_cost - v_accrued_unit) * 100.0 / v_accrued_unit;
        IF v_diff_pct > v_tolerance_pct THEN
          RAISE EXCEPTION
            'ap_bill_three_way_mismatch: line % unit_cost % differs from '
            'accrued unit_value % by %%% (vendor tolerance %%%)',
            v_idx, v_unit_cost, v_accrued_unit, v_diff_pct, v_tolerance_pct
            USING ERRCODE = 'P0024';
        END IF;
      END IF;
      IF v_amount <> v_qty * v_unit_cost THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % amount % <> qty % × unit_cost %',
          v_idx, v_amount, v_qty, v_unit_cost
          USING ERRCODE = 'P0024';
      END IF;

      SELECT COALESCE(SUM(qty), 0) INTO v_total_billed
        FROM vendor_bill_lines
       WHERE kind = 'disposal_match'
         AND disposal_wo_event_id = v_disp_event_id
         AND by_product_no = v_by_product_no;
      v_avail := v_bp.actual_qty - v_total_billed;
      IF v_qty > v_avail THEN
        RAISE EXCEPTION
          'ap_bill_three_way_mismatch: line % qty % exceeds accrued-not-'
          'billed remainder % for wo_by_products(wo=%,no=%) (accrued=%, '
          'already billed=%)',
          v_idx, v_qty, v_avail, v_disp_wo_id, v_by_product_no,
          v_bp.actual_qty, v_total_billed
          USING ERRCODE = 'P0024';
      END IF;

      SELECT id INTO v_disp_liability FROM accounts
       WHERE kind = 'accrued_disposal_liability'
         AND counterparty_id = p_vendor_id
         AND currency = p_currency
         AND NOT is_closed;
      IF v_disp_liability IS NULL THEN
        RAISE EXCEPTION
          'no open accrued_disposal_liability account for vendor=% ccy=%',
          p_vendor_id, p_currency USING ERRCODE = 'P0010';
      END IF;

      INSERT INTO vendor_bill_lines (
        bill_id, line_no, kind,
        disposal_wo_event_id, by_product_no,
        qty, unit_cost, amount
      ) VALUES (
        v_doc_id, v_idx, 'disposal_match',
        v_disp_event_id, v_by_product_no,
        v_qty, v_unit_cost, v_amount
      ) RETURNING id INTO v_bill_line_id;

      v_amount_at_accrual := v_qty * v_accrued_unit;
      v_batch := v_batch || jsonb_build_array(jsonb_build_object(
        'reason',            'ap_bill',
        'document_kind',     'vendor_bill',
        'document_id',       v_doc_id,
        'document_line_id',  v_bill_line_id,
        'debit_account_id',  v_disp_liability,
        'credit_account_id', v_ven_ap,
        'amount',            v_amount_at_accrual,
        'business_date',     p_business_date,
        'idempotency_key',   gen_random_uuid(),
        'counterparty_id',   p_vendor_id,
        'posted_by',         p_posted_by
      ));

      v_diff_total := v_amount - v_amount_at_accrual;
      IF v_diff_total <> 0 THEN
        SELECT id INTO v_match_tol_acct FROM accounts
         WHERE kind='variance_match_tolerance' AND ledger_kind='value'
           AND currency=p_currency AND NOT is_closed;
        IF v_match_tol_acct IS NULL THEN
          RAISE EXCEPTION 'no open variance_match_tolerance account for ccy=%',
                          p_currency USING ERRCODE = 'P0010';
        END IF;

        IF v_diff_total > 0 THEN
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ap_bill',
            'document_kind',     'vendor_bill',
            'document_id',       v_doc_id,
            'document_line_id',  v_bill_line_id,
            'debit_account_id',  v_match_tol_acct,
            'credit_account_id', v_ven_ap,
            'amount',            v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        ELSE
          v_batch := v_batch || jsonb_build_array(jsonb_build_object(
            'reason',            'ap_bill',
            'document_kind',     'vendor_bill',
            'document_id',       v_doc_id,
            'document_line_id',  v_bill_line_id,
            'debit_account_id',  v_ven_ap,
            'credit_account_id', v_match_tol_acct,
            'amount',            -v_diff_total,
            'business_date',     p_business_date,
            'idempotency_key',   gen_random_uuid(),
            'counterparty_id',   p_vendor_id,
            'posted_by',         p_posted_by
          ));
        END IF;
      END IF;

    ELSE
      RAISE EXCEPTION 'ap_bill_invalid_line: line % unknown kind %',
                      v_idx, v_kind USING ERRCODE = 'P0025';
    END IF;
  END LOOP;

  PERFORM post_posting_lines(v_batch, FALSE);
  RETURN v_doc_id;
END;
$$;
