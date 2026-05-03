-- acct-0kl / BOM2 — consumption_policy enforcement gate.
--
-- skus.consumption_policy was added in migration 0040 (A1) as a
-- placeholder column with values ('forward','backflush_at_op',
-- 'backflush_at_complete'). Only 'forward' is dispatched today —
-- the backflush variants are deferred to acct-BACKFLUSH (Phase 2).
--
-- The plan calls for raising P0035 'consumption_policy_not_implemented'
-- when a backflush variant is in use. Implementing this as a
-- BEFORE INSERT trigger on wo_events (filtering for event_kind='start')
-- is a much smaller diff than re-issuing post_wo_start with the gate
-- inlined: the trigger composes uniformly across the new BOM2 path
-- and the legacy Slice B `boms` path, both of which insert a 'start'
-- row into wo_events.
--
-- Once op_move / scrap / wo_complete run for a WO, wo_start has
-- already passed the check, so no further gating is needed there.

CREATE OR REPLACE FUNCTION _wo_events_check_consumption_policy()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_policy TEXT;
  v_parent UUID;
BEGIN
  IF NEW.event_kind <> 'start' THEN
    RETURN NEW;
  END IF;

  SELECT s.consumption_policy, w.parent_sku_id
    INTO v_policy, v_parent
    FROM work_orders w
    JOIN skus s ON s.id = w.parent_sku_id
   WHERE w.id = NEW.wo_id;

  IF v_policy <> 'forward' THEN
    RAISE EXCEPTION
      'consumption_policy_not_implemented: parent_sku=% consumption_policy=% — '
      'only ''forward'' is dispatched today (Phase 2 acct-BACKFLUSH)',
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

COMMENT ON FUNCTION _wo_events_check_consumption_policy() IS
  'P0035 gate: raises when a WO''s parent_sku has a non-forward '
  'consumption_policy. Reserves the column for acct-BACKFLUSH Phase 2 '
  'while keeping today''s ''forward'' default working. acct-0kl.';
