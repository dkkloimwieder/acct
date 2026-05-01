-- Revert acct-8qi / acct-og1.1.
-- Restores 0031's close_period (1-arg call to cost_adjust_retroactive_hook),
-- restores cost_adjust_retroactive_hook to the s6n stub (1-arg, RETURN 0),
-- drops the queue table and entry function.

-- Drop the 2-arg real body first.
DROP FUNCTION IF EXISTS cost_adjust_retroactive_hook(BIGINT, BOOLEAN);

-- Restore s6n stub.
CREATE OR REPLACE FUNCTION cost_adjust_retroactive_hook(p_period_id BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS $$
BEGIN
  RETURN 0;
END;
$$;

-- Restore close_period (1-arg call to cost_adjust_retroactive_hook).
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
  v_wac_period_count       BIGINT;
  v_wac_retro_count        BIGINT;
  v_cost_adj_retro_count   BIGINT;
  v_finalized_count        BIGINT;
  v_unfinalized_remaining  BIGINT;
  v_alerts                 INT;
  v_now                    TIMESTAMPTZ;
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
  v_wac_period_count     := wac_periodic_close_hook(p_period_id, p_force_provisional);
  v_wac_retro_count      := wac_retroactive_close_hook(p_period_id, p_force_provisional);
  v_cost_adj_retro_count := cost_adjust_retroactive_hook(p_period_id);
  v_finalized_count      := v_wac_period_count + v_wac_retro_count + v_cost_adj_retro_count;
  SELECT COUNT(*) INTO v_unfinalized_remaining
    FROM transfers_provisional WHERE period_id = p_period_id AND finalized_at IS NULL;
  IF v_unfinalized_remaining > 0 AND NOT p_force_provisional THEN
    RAISE EXCEPTION 'period_close_provisional: % un-finalized provisional rows remain in period % (id=%); pass p_force_provisional=TRUE to override',
      v_unfinalized_remaining, v_period_code, p_period_id USING ERRCODE = 'P0015';
  END IF;
  v_alerts := run_daily_reconciliation();
  IF v_alerts > 0 AND NOT p_force_recon THEN
    RAISE EXCEPTION 'period_close_reconciliation: % new reconciliation alert(s) raised while closing period % (id=%); pass p_force_recon=TRUE to override',
      v_alerts, v_period_code, p_period_id USING ERRCODE = 'P0016';
  END IF;
  v_now := clock_timestamp();
  UPDATE periods SET closed_at = v_now, closed_by = p_actor WHERE id = p_period_id;
  RETURN jsonb_build_object(
    'period_id', p_period_id, 'period_code', v_period_code,
    'closed_at', v_now, 'closed_by', p_actor,
    'finalized_count', v_finalized_count,
    'hook_results', jsonb_build_object(
      'wac_periodic', v_wac_period_count,
      'wac_retroactive', v_wac_retro_count,
      'cost_adjust_retroactive', v_cost_adj_retro_count),
    'unfinalized_remaining', v_unfinalized_remaining,
    'alerts', v_alerts,
    'forced', jsonb_build_object('provisional', p_force_provisional, 'recon', p_force_recon));
END;
$$;

-- Drop entry function and queue table.
DROP FUNCTION IF EXISTS post_cost_adjustment_retroactive(
  BIGINT, UUID, UUID, TEXT, TEXT, BIGINT, DATE, UUID, UUID, TEXT
);

DROP TABLE IF EXISTS inventory_cost_adjustments_retroactive;
