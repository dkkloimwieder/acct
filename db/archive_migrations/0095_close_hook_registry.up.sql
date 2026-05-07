-- acct-w0lo (2/2) — close-hook registry, mirroring the dispatcher
-- registry from mig 0094.
--
-- Why:
-- ----
-- close_period() hard-codes three close hooks in a fixed order:
--   1. wac_periodic_close_hook
--   2. wac_retroactive_close_hook
--   3. cost_adjust_retroactive_hook  (must run last; layers against
--      the WAC hooks' already-finalized provisional state)
--
-- Adding a 4th close hook (e.g. fifo_close_hook for acct-8gg) means
-- editing close_period. Same 'monolithic switch' pattern that mig 0094
-- broke for the dispatcher.
--
-- This migration introduces `close_hooks(hook_fn_name, ordering,
-- result_key, ...)`. close_period iterates the registry in `ordering`
-- order, EXECUTE-formats each, and assembles the JSONB hook_results
-- object from `result_key`s.
--
-- Ordering semantics:
-- -------------------
-- Lower `ordering` runs first. Gaps between values are intentional —
-- registering an in-between hook (e.g. ordering=15) interleaves cleanly.
-- ties on ordering are deterministic by hook_fn_name (PK).
--
-- Adding FIFO (acct-8gg) becomes: write fifo_close_hook(p_period_id,
-- p_force_provisional) RETURNS BIGINT, then INSERT a registry row at an
-- appropriate ordering. No close_period edit.
--
-- Hook signature contract:
--   FUNCTION foo_close_hook(p_period_id BIGINT, p_force_provisional BOOLEAN)
--     RETURNS BIGINT
--   Returns: count of provisional rows finalized (or rows flushed for
--   non-WAC hooks). Used to populate hook_results[result_key] and
--   accumulate into finalized_count.
--
-- Class-confusion checklist (R1-R7):
-- - R5/R6 not applicable (registry is config, not pool / idempotency).
-- - R7 not applicable (close_period builds a JSONB result; doesn't
--   persist a document audit field).

-- ============================================================
-- 1. Registry table.
-- ============================================================

CREATE TABLE close_hooks (
  hook_fn_name   TEXT        NOT NULL PRIMARY KEY,
  ordering       INT         NOT NULL,
  result_key     TEXT        NOT NULL,
  description    TEXT,
  registered_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (ordering >= 0)
);

CREATE INDEX close_hooks_ordering_idx ON close_hooks (ordering, hook_fn_name);

COMMENT ON TABLE close_hooks IS
  'acct-w0lo registry of period-close hooks. close_period iterates rows
   in ordering order and EXECUTEs each as
   foo_close_hook(p_period_id, p_force_provisional) RETURNS BIGINT.
   The returned count is placed in the JSONB hook_results object under
   result_key and accumulated into finalized_count. Adding a new hook
   = INSERT row + create function; no close_period edit.';

-- ============================================================
-- 2. Register existing hooks at their current ordering.
--
-- 10 / 20 / 30 with 10-unit gaps so a new hook can interleave at 15
-- (between wac_periodic and wac_retroactive) or 25 (between
-- wac_retroactive and cost_adjust_retroactive) without renumbering.
-- ============================================================

INSERT INTO close_hooks (hook_fn_name, ordering, result_key, description)
VALUES
  ('wac_periodic_close_hook',       10, 'wac_periodic',
   'Recompute period-end avg per pool; finalize wac_periodic provisional rows.'),
  ('wac_retroactive_close_hook',    20, 'wac_retroactive',
   'Topological + chronological replay; finalize wac_retroactive provisional rows.'),
  ('cost_adjust_retroactive_hook',  30, 'cost_adjust_retroactive',
   'Flush queued retroactive cost-adjust rows; method-agnostic, runs after WAC hooks so it layers on top.');

-- ============================================================
-- 3. Refactor close_period to iterate the registry.
--
-- Behavior preservation:
-- - Same gates: P0014 (period missing/closed), P0015 (un-finalized
--   provisional), P0016 (recon alerts).
-- - Same JSONB return shape: { period_id, period_code, closed_at,
--   closed_by, finalized_count, hook_results: {key: count, ...},
--   unfinalized_remaining, alerts, forced: {provisional, recon} }.
-- - Same FOR UPDATE on the period row to serialize concurrent callers.
-- - Same ordering of side effects: hooks → un-finalized check → recon →
--   stamp closed_at.
--
-- Differences:
-- - hook_results is built dynamically from registry result_keys instead
--   of jsonb_build_object('wac_periodic', n1, ...) literally.
-- - Adding a new hook does not require editing this function.
-- ============================================================

CREATE OR REPLACE FUNCTION close_period(
  p_period_id         BIGINT,
  p_actor             UUID,
  p_force_provisional BOOLEAN DEFAULT FALSE,
  p_force_recon       BOOLEAN DEFAULT FALSE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_period_code            TEXT;
  v_already_closed         TIMESTAMPTZ;
  v_finalized_count        BIGINT := 0;
  v_unfinalized_remaining  BIGINT;
  v_alerts                 INT;
  v_now                    TIMESTAMPTZ;
  v_hook_results           JSONB := '{}'::JSONB;
  v_hook                   RECORD;
  v_hook_count             BIGINT;
BEGIN
  SELECT code, closed_at INTO v_period_code, v_already_closed
    FROM periods WHERE id = p_period_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'period_close_invalid: period id=% not found', p_period_id USING ERRCODE = 'P0014';
  END IF;
  IF v_already_closed IS NOT NULL THEN
    RAISE EXCEPTION 'period_close_invalid: period % (id=%) already closed at %',
      v_period_code, p_period_id, v_already_closed USING ERRCODE = 'P0014';
  END IF;

  -- Iterate registered hooks in ordering order. Each hook returns BIGINT
  -- (count finalized / flushed); we accumulate into finalized_count and
  -- place under result_key in the hook_results object.
  FOR v_hook IN
    SELECT hook_fn_name, result_key
      FROM close_hooks
     ORDER BY ordering, hook_fn_name
  LOOP
    EXECUTE format('SELECT %I($1, $2)', v_hook.hook_fn_name)
      INTO v_hook_count
      USING p_period_id, p_force_provisional;
    v_hook_results := v_hook_results || jsonb_build_object(v_hook.result_key, v_hook_count);
    v_finalized_count := v_finalized_count + COALESCE(v_hook_count, 0);
  END LOOP;

  SELECT COUNT(*) INTO v_unfinalized_remaining
    FROM transfers_provisional
   WHERE period_id = p_period_id AND finalized_at IS NULL;

  IF v_unfinalized_remaining > 0 AND NOT p_force_provisional THEN
    RAISE EXCEPTION
      'period_close_provisional: % un-finalized provisional rows '
      'remain in period % (id=%); pass p_force_provisional=TRUE to override',
      v_unfinalized_remaining, v_period_code, p_period_id USING ERRCODE = 'P0015';
  END IF;

  v_alerts := run_daily_reconciliation();
  IF v_alerts > 0 AND NOT p_force_recon THEN
    RAISE EXCEPTION
      'period_close_reconciliation: % new reconciliation alert(s) raised '
      'while closing period % (id=%); pass p_force_recon=TRUE to override',
      v_alerts, v_period_code, p_period_id USING ERRCODE = 'P0016';
  END IF;

  v_now := clock_timestamp();
  UPDATE periods SET closed_at = v_now, closed_by = p_actor WHERE id = p_period_id;

  RETURN jsonb_build_object(
    'period_id', p_period_id, 'period_code', v_period_code,
    'closed_at', v_now, 'closed_by', p_actor,
    'finalized_count', v_finalized_count,
    'hook_results', v_hook_results,
    'unfinalized_remaining', v_unfinalized_remaining,
    'alerts', v_alerts,
    'forced', jsonb_build_object('provisional', p_force_provisional, 'recon', p_force_recon)
  );
END;
$$;

COMMENT ON FUNCTION close_period(BIGINT, UUID, BOOLEAN, BOOLEAN) IS
  'acct-w0lo registry-driven period close. Iterates close_hooks in
   ordering order, EXECUTEs each as
   foo_close_hook(p_period_id, p_force_provisional), aggregates results
   into hook_results JSONB. Same gates / errcodes / return shape as the
   pre-refactor close_period from mig 0032.';
