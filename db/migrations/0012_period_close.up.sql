-- Period-close orchestration.
--
-- Consolidates archive migs 0025 (transfers_provisional + variance enums)
-- + 0026 (close_period orchestrator + hook stubs) + 0032 (retroactive
-- queue) + 0065 (CHECK relax for tier-2 internal-chain rows) + 0095
-- (registry-driven close_period).
--
-- Naming unifications baked in:
--   - transfers_provisional → posting_lines_provisional
--   - transfer_id → posting_line_id
--   - variance_transfer_id → variance_posting_line_id
--
-- The 3 hook functions (wac_periodic_close_hook,
-- wac_retroactive_close_hook, cost_adjust_retroactive_hook) are
-- defined in 0015 with their final bodies. The close_hooks registry
-- is seeded in 0021 (after the hook functions exist).

-- ============================================================
-- posting_lines_provisional: side table flagging posting_lines as
-- having provisional cost components that must be finalized at close.
-- ============================================================

CREATE TABLE posting_lines_provisional (
  posting_line_id            BIGINT PRIMARY KEY REFERENCES posting_lines(id),
  period_id                  BIGINT NOT NULL REFERENCES periods(id),
  cost_method                cost_method NOT NULL,
  finalized_at               TIMESTAMPTZ,
  variance_amount            BIGINT,
  variance_posting_line_id   BIGINT REFERENCES posting_lines(id),
  qty                        BIGINT,

  -- CHECK relaxed in mig 0065 to permit non-zero variance with NULL
  -- variance_posting_line_id (tier-2 internal-chain op_move_v / rm_issue_to_wo
  -- finalizations record variance but DO NOT post a transfer).
  CHECK (
    CASE
      WHEN finalized_at IS NULL THEN
        variance_amount IS NULL AND variance_posting_line_id IS NULL
      WHEN variance_amount IS NULL THEN
        FALSE
      WHEN variance_amount = 0 THEN
        variance_posting_line_id IS NULL
      ELSE
        TRUE
    END
  ),
  CHECK (variance_posting_line_id IS NULL
         OR variance_posting_line_id <> posting_line_id)
);

-- Hot path for close_period: "find every un-finalized row in this period".
CREATE INDEX plp_period_unfinalized
  ON posting_lines_provisional (period_id)
  WHERE finalized_at IS NULL;

CREATE INDEX plp_finalized_at
  ON posting_lines_provisional (finalized_at)
  WHERE finalized_at IS NOT NULL;

COMMENT ON TABLE posting_lines_provisional IS
  'Side table marking posting_lines as having provisional cost '
  'components (wac_periodic / wac_retroactive depletions). The '
  'append-only trigger on posting_lines blocks UPDATE/DELETE on the '
  'posting line itself; this table carries the lifecycle (finalized_at, '
  'variance_amount, variance_posting_line_id). Internal-chain '
  'finalizations (acct-smn / acct-rso) record variance with '
  'variance_posting_line_id NULL (no transfer posted; cost shift '
  'propagates to leaf depletions via LEFT JOIN cache).';

-- ============================================================
-- inventory_cost_adjustments_retroactive: queue table for
-- post_cost_adjustment_retroactive (mig 0032 / acct-og1).
-- ============================================================

CREATE TABLE inventory_cost_adjustments_retroactive (
  id                UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  sku_id            UUID NOT NULL REFERENCES skus(id),
  location_id       UUID NOT NULL REFERENCES locations(id),
  currency          TEXT NOT NULL,
  inventory_class   TEXT NOT NULL CHECK (inventory_class IN ('raw','fg')),
  target_period_id  BIGINT NOT NULL REFERENCES periods(id),
  target_avg        BIGINT NOT NULL CHECK (target_avg >= 0),
  business_date     DATE NOT NULL,
  posted_by         UUID NOT NULL,
  posted_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  finalized_at      TIMESTAMPTZ,
  finalized_count   BIGINT,
  total_variance    BIGINT,
  idempotency_key   UUID NOT NULL UNIQUE,
  notes             TEXT
);

CREATE INDEX inv_cost_adj_retro_target_period_unfinalized
  ON inventory_cost_adjustments_retroactive (target_period_id)
  WHERE finalized_at IS NULL;

CREATE INDEX inv_cost_adj_retro_sku_period
  ON inventory_cost_adjustments_retroactive (sku_id, target_period_id);

CREATE INDEX inv_cost_adj_retro_posted_at
  ON inventory_cost_adjustments_retroactive (posted_at);

-- ============================================================
-- close_hooks registry (acct-w0lo, was mig 0095).
-- ============================================================

CREATE TABLE close_hooks (
  hook_fn_name   TEXT        NOT NULL PRIMARY KEY,
  ordering       INT         NOT NULL,
  result_key     TEXT        NOT NULL,
  description    TEXT,
  registered_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (ordering >= 0)
);

CREATE INDEX close_hooks_ordering_idx
  ON close_hooks (ordering, hook_fn_name);

COMMENT ON TABLE close_hooks IS
  'Registry of period-close hooks. close_period iterates rows in '
  'ordering order and EXECUTEs each as foo_close_hook(p_period_id, '
  'p_force_provisional) RETURNS BIGINT. Adding a new hook = INSERT '
  'row + create function; no close_period edit. Seeded in 0021 with '
  'the 3 standard hooks (ordering 10/20/30, 10-unit gaps).';

-- ============================================================
-- close_period: registry-driven orchestrator (acct-w0lo final form).
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
    RAISE EXCEPTION 'period_close_invalid: period id=% not found', p_period_id
      USING ERRCODE = 'P0014';
  END IF;
  IF v_already_closed IS NOT NULL THEN
    RAISE EXCEPTION 'period_close_invalid: period % (id=%) already closed at %',
      v_period_code, p_period_id, v_already_closed
      USING ERRCODE = 'P0014';
  END IF;

  -- Iterate registered hooks in ordering order. Each hook returns BIGINT
  -- (count finalized / flushed); accumulate into finalized_count and
  -- place under result_key in hook_results.
  FOR v_hook IN
    SELECT hook_fn_name, result_key
      FROM close_hooks
     ORDER BY ordering, hook_fn_name
  LOOP
    EXECUTE format('SELECT %I($1, $2)', v_hook.hook_fn_name)
      INTO v_hook_count
      USING p_period_id, p_force_provisional;
    v_hook_results := v_hook_results
                      || jsonb_build_object(v_hook.result_key, v_hook_count);
    v_finalized_count := v_finalized_count + COALESCE(v_hook_count, 0);
  END LOOP;

  SELECT COUNT(*) INTO v_unfinalized_remaining
    FROM posting_lines_provisional
   WHERE period_id = p_period_id AND finalized_at IS NULL;

  IF v_unfinalized_remaining > 0 AND NOT p_force_provisional THEN
    RAISE EXCEPTION
      'period_close_provisional: % un-finalized provisional rows '
      'remain in period % (id=%); pass p_force_provisional=TRUE to override',
      v_unfinalized_remaining, v_period_code, p_period_id
      USING ERRCODE = 'P0015';
  END IF;

  v_alerts := run_daily_reconciliation();
  IF v_alerts > 0 AND NOT p_force_recon THEN
    RAISE EXCEPTION
      'period_close_reconciliation: % new reconciliation alert(s) raised '
      'while closing period % (id=%); pass p_force_recon=TRUE to override',
      v_alerts, v_period_code, p_period_id
      USING ERRCODE = 'P0016';
  END IF;

  v_now := clock_timestamp();
  UPDATE periods SET closed_at = v_now, closed_by = p_actor
    WHERE id = p_period_id;

  RETURN jsonb_build_object(
    'period_id',             p_period_id,
    'period_code',           v_period_code,
    'closed_at',             v_now,
    'closed_by',             p_actor,
    'finalized_count',       v_finalized_count,
    'hook_results',          v_hook_results,
    'unfinalized_remaining', v_unfinalized_remaining,
    'alerts',                v_alerts,
    'forced', jsonb_build_object('provisional', p_force_provisional,
                                  'recon',       p_force_recon)
  );
END;
$$;

COMMENT ON FUNCTION close_period(BIGINT, UUID, BOOLEAN, BOOLEAN) IS
  'Registry-driven period close (acct-w0lo). Iterates close_hooks in '
  'ordering, EXECUTEs each as foo_close_hook(p_period_id, '
  'p_force_provisional), aggregates into hook_results JSONB. P0014 '
  '(missing/already-closed), P0015 (un-finalized), P0016 (recon).';
