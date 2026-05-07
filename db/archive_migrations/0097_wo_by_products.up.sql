-- acct-v5r6 / acct-7t4.2 — wo_by_products: per-WO snapshot of
-- bom_by_products at wo_start.
--
-- Snapshot pattern mirrors wo_outputs. The bom_by_products row is the
-- BOM-side declaration; wo_by_products freezes the per-WO planned_qty
-- + economics so subsequent BOM revisions don't retroactively change
-- already-started WOs (mirror of bom_lines snapshot semantics).
--
-- planned_qty is set at INSERT time as ROUND(qty_per_parent × qty_target).
-- A BEFORE UPDATE trigger blocks any change to planned_qty after creation
-- — once a WO is in flight, the plan is fixed and yield variance at
-- wo_complete compares (actual_qty − planned_qty).
--
-- actual_qty defaults to planned_qty at INSERT and is mutable. Caller
-- updates before wo_complete to assert what was actually produced.
-- Updating after wo_complete is permitted at the table level (the
-- consequences for already-posted ledger are caller's problem).
--
-- post_wo_start auto-inits one wo_by_products row per bom_by_products
-- row of the resolved BOM. If the caller pre-populated rows for the WO
-- (ad-hoc by-products without BOM declaration), the auto-init is a
-- no-op — caller intent wins, mirroring wo_outputs auto-init.
--
-- New error code:
--   P0051 wo_by_product_immutable — UPDATE of planned_qty rejected.

-- ============================================================
-- 1. wo_by_products
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

COMMENT ON TABLE wo_by_products IS
  'Per-WO snapshot of bom_by_products at wo_start. planned_qty fixed; '
  'actual_qty mutable until wo_complete drains it. Yield variance at '
  'wo_complete = (actual − planned) × unit_value. acct-7t4.2.';

COMMENT ON COLUMN wo_by_products.planned_qty IS
  'Snapshotted at wo_start as ROUND(bom_by_products.qty_per_parent × '
  'wo.qty_target). Immutable thereafter (see wo_by_products_planned_qty_'
  'immutable trigger).';

COMMENT ON COLUMN wo_by_products.actual_qty IS
  'Defaults to planned_qty at INSERT. Caller UPDATEs before wo_complete '
  'to assert actual yield. Variance posted to variance_yield_byproduct '
  'at wo_complete (acct-7t4.6).';

-- ============================================================
-- 2. planned_qty immutability trigger
-- ============================================================

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

COMMENT ON FUNCTION wo_by_products_planned_qty_immutable() IS
  'Trigger fn: rejects UPDATE of planned_qty on wo_by_products. '
  'planned_qty is the snapshot taken at wo_start; mutating it would '
  'silently change yield variance math at wo_complete. P0051.';

-- ============================================================
-- 3. post_wo_start: snapshot bom_by_products → wo_by_products
-- ============================================================
--
-- Diff vs mig 0070 is a single new block after the wo_outputs auto-init:
-- read bom_by_products for the resolved BOM and INSERT wo_by_products
-- rows IFF none exist for this WO. Caller pre-populated rows win;
-- BOM-driven snapshot fills in only if the caller didn't.
--
-- Note: caller-pre-populate-then-also-snapshot is NOT supported (per
-- the umbrella's "caller intent wins" semantics — same as wo_outputs).
-- If the caller wants both ad-hoc and BOM-declared by-products, they
-- can pre-populate the ad-hoc ones AND copy bom_by_products manually
-- with picked by_product_no values — this function will see a non-
-- empty wo_by_products and skip auto-init entirely. Keeps the snapshot
-- semantics simple and predictable.

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
      'wo_invalid: parent_sku % has cost_method=% which post_wo_start '
      'does not handle',
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

  -- acct-7t4.2: snapshot bom_by_products → wo_by_products. Only when
  -- the caller has not pre-populated wo_by_products for ad-hoc by-
  -- products. Caller intent wins; same shape as wo_outputs auto-init
  -- above.
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
     -- Skip rows that would round to zero (qty_per_parent very small +
     -- qty_target small could underflow planned_qty CHECK > 0).
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
    'document_kind',     'wo_event',
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
    v_event_id, p_business_date, p_posted_by
  );

  v_batch := v_batch || _wo_emit_bom_lines(
    p_wo_id, v_bom.id, v_first_op, v_wo.qty_target,
    jsonb_build_object('fire_at', 'op_arrival', 'applies_at_op', v_first_op),
    v_event_id, p_business_date, p_posted_by
  );

  PERFORM post_transfers(v_batch, FALSE);
  UPDATE work_orders SET status = 'released' WHERE id = p_wo_id;
  RETURN p_wo_id;
END;
$$;

COMMENT ON FUNCTION post_wo_start(UUID, DATE, UUID, UUID, TEXT) IS
  'WO lifecycle entry. Validates idempotency / status / BOM / routing / '
  'cost_method. Auto-inits wo_outputs (single primary output if empty) '
  'and wo_by_products (snapshot from bom_by_products if empty). Both '
  'auto-inits are no-ops when the caller pre-populated rows. acct-7t4.2.';
