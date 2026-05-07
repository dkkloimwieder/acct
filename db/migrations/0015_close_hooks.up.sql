-- Period-close hook function bodies.
--
-- Consolidates archive migs:
--   0029 (acct-qfj)  — wac_periodic_close_hook initial body
--   0031 (acct-9tw)  — wac_retroactive_close_hook initial body
--   0032 (acct-og1)  — cost_adjust_retroactive_hook real body
--   0064 (acct-bol)  — _wac_close_pool_qty_in helper
--   0065 (acct-smn)  — wac_periodic topological pool walk
--   0067 (acct-7py)  — rm_issue_to_wo internal-chain edges
--   0070 (acct-rso)  — wac_retroactive merged value/qty stream
--   0077 (acct-7eo)  — mixed-cost-method variance_material_mixed routing
--
-- Naming unifications baked in:
--   transfers → posting_lines
--   transfers_provisional → posting_lines_provisional
--   transfer_id (FK column) → posting_line_id
--   variance_transfer_id → variance_posting_line_id
--   transfer_reason (type) → posting_line_reason
--   _post_transfers_* → _post_posting_lines_*
--   post_transfers (call site) → post_posting_lines
--   variance_wac_period → variance_wac_periodic
--   variance_cost_adjust_retro → variance_cost_adjust_retroactive
--
-- The 3 hook functions all share the signature
--   (p_period_id BIGINT, p_force_provisional BOOLEAN DEFAULT FALSE) RETURNS BIGINT
-- so they can be invoked uniformly by the close_hooks registry from 0012.
-- Registry seeds in 0021.
--
-- close_period itself is already defined in 0012 (registry-driven). The
-- 3 hook functions register into close_hooks at orderings 10/20/30 in 0021.

-- ============================================================
-- _wac_close_pool_qty_in — pool_qty_in dispatch on pool.kind
--
-- For inv_value_wip pools, the per-class qty pattern breaks because
-- rm_issue_to_wo value-leg stores qty = component qty consumed, not
-- parent qty received. So we read parent qty inflows from the matching
-- stock_wip account (per-(sku, op), no class sharing).
-- For raw / fg pools the per-class pattern stays correct.
-- ============================================================

CREATE OR REPLACE FUNCTION _wac_close_pool_qty_in(
  p_pool_acct      accounts,
  p_period_opens   DATE,
  p_period_closes  DATE
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_qty_acct_id  BIGINT;
  v_qty_in       BIGINT;
BEGIN
  IF p_pool_acct.kind = 'inv_value_wip' THEN
    v_qty_acct_id := _post_posting_lines_lookup_qty_account(p_pool_acct);
    IF v_qty_acct_id IS NULL THEN
      RETURN NULL;
    END IF;
    SELECT COALESCE(SUM(t.qty), 0) INTO v_qty_in
      FROM posting_lines t
     WHERE t.debit_account_id = v_qty_acct_id
       AND t.business_date BETWEEN p_period_opens AND p_period_closes
       AND t.qty IS NOT NULL;
    RETURN v_qty_in;
  ELSE
    SELECT COALESCE(SUM(t.qty), 0) INTO v_qty_in
      FROM posting_lines t
     WHERE t.debit_account_id = p_pool_acct.id
       AND t.business_date BETWEEN p_period_opens AND p_period_closes
       AND t.qty IS NOT NULL;
    RETURN v_qty_in;
  END IF;
END;
$$;

-- ============================================================
-- wac_periodic_close_hook
--
-- Topological per-pool recompute. Pool set = credit accounts of
-- flagged depletions UNION debit accounts of internal-chain reasons
-- (op_move_v / rm_issue_to_wo) so successor pools get visited.
-- Edge set = (credit, debit) of internal-chain rows. Kahn-sorted walk.
--
-- Per pool: corrected_value_in via LEFT JOIN on variance_amount cache
-- (filtered to variance_posting_line_id IS NULL); final_avg from
-- _wac_close_pool_qty_in.
--
-- Per provisional row in the pool:
--   - Mixed-method rm_issue_to_wo (acct-7eo): single-leg variance
--     through variance_material_mixed at the component pool;
--     destination WIP untouched (R5 — debit-normal pool drained).
--   - Homogeneous internal-chain (op_move_v / rm_issue_to_wo with
--     wac_periodic destination): record variance, no transfer.
--   - Leaf depletion on inv_value_wip source: single-leg variance
--     between orig_debit and variance_wac_periodic.
--   - Leaf depletion on raw/fg source: 2-leg wash through
--     variance_wac_periodic.
--
-- Rework cycles → P0036.
-- ============================================================

CREATE OR REPLACE FUNCTION wac_periodic_close_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_pool_id        BIGINT;
  v_processed      BIGINT[] := ARRAY[]::BIGINT[];
  v_remaining      INT;
  v_progress       INT;
  v_cycle_pools    TEXT;
  v_row            RECORD;
  v_orig           RECORD;
  v_pool_acct      accounts%ROWTYPE;
  v_pool_value_in  BIGINT;
  v_pool_qty_in    BIGINT;
  v_final_avg      BIGINT;
  v_provisional    BIGINT;
  v_variance       BIGINT;
  v_var_acct       BIGINT;
  v_batch          JSONB;
  v_var_xfer_id    BIGINT;
  v_orig_debit     BIGINT;
  v_orig_credit    BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
  v_orig_reason    posting_line_reason;
  v_dest_method    TEXT;
  v_mixed          BOOLEAN;
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN
    RETURN 0;
  END IF;

  CREATE TEMP TABLE _wac_pools (
    pool_id BIGINT PRIMARY KEY
  ) ON COMMIT DROP;

  CREATE TEMP TABLE _wac_edges (
    predecessor BIGINT,
    successor   BIGINT,
    PRIMARY KEY (predecessor, successor)
  ) ON COMMIT DROP;

  INSERT INTO _wac_pools (pool_id)
  SELECT DISTINCT t.credit_account_id
    FROM posting_lines_provisional p
    JOIN posting_lines t ON t.id = p.posting_line_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
  UNION
  SELECT DISTINCT t.debit_account_id
    FROM posting_lines_provisional p
    JOIN posting_lines t ON t.id = p.posting_line_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  INSERT INTO _wac_edges (predecessor, successor)
  SELECT DISTINCT t.credit_account_id, t.debit_account_id
    FROM posting_lines_provisional p
    JOIN posting_lines t ON t.id = p.posting_line_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_periodic'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  LOOP
    SELECT COUNT(*) INTO v_remaining FROM _wac_pools;
    EXIT WHEN v_remaining = 0;

    v_progress := 0;
    FOR v_pool_id IN
      SELECT wp.pool_id
        FROM _wac_pools wp
       WHERE NOT EXISTS (
         SELECT 1 FROM _wac_edges e
          WHERE e.successor = wp.pool_id
            AND e.predecessor IN (SELECT pool_id FROM _wac_pools)
       )
    LOOP
      v_progress := v_progress + 1;

      SELECT * INTO v_pool_acct FROM accounts WHERE id = v_pool_id;

      SELECT COALESCE(SUM(
        t.amount + COALESCE(p.variance_amount, 0)
      ), 0) INTO v_pool_value_in
        FROM posting_lines t
        LEFT JOIN posting_lines_provisional p
               ON p.posting_line_id = t.id
              AND p.finalized_at IS NOT NULL
              AND p.variance_posting_line_id IS NULL
       WHERE t.debit_account_id = v_pool_id
         AND t.business_date BETWEEN v_period_opens AND v_period_closes;

      v_pool_qty_in := _wac_close_pool_qty_in(
        v_pool_acct, v_period_opens, v_period_closes
      );
      IF v_pool_qty_in IS NULL THEN
        RAISE EXCEPTION
          'wac_periodic_close: cannot resolve qty account for value pool %',
          v_pool_id USING ERRCODE = 'P0010';
      END IF;

      IF v_pool_qty_in = 0 THEN
        IF p_force_provisional THEN
          DELETE FROM _wac_pools WHERE pool_id = v_pool_id;
          v_processed := array_append(v_processed, v_pool_id);
          CONTINUE;
        END IF;
        RAISE EXCEPTION
          'wac_periodic_close_no_receipts: period % (id=%) has provisional '
          'depletions on pool kind=% sku=% loc=% op=% ccy=% but zero receipts in '
          'period; post receipts and retry the close, or close with '
          'p_force_provisional=TRUE.',
          v_period_code, p_period_id, v_pool_acct.kind, v_pool_acct.sku_id,
          v_pool_acct.location_id, v_pool_acct.routing_op, v_pool_acct.currency
          USING ERRCODE = 'P0020';
      END IF;

      v_final_avg := v_pool_value_in / v_pool_qty_in;

      FOR v_row IN
        SELECT *
          FROM posting_lines_provisional
         WHERE period_id = p_period_id
           AND cost_method = 'wac_periodic'
           AND finalized_at IS NULL
         ORDER BY posting_line_id
           FOR UPDATE
      LOOP
        SELECT * INTO v_orig FROM posting_lines WHERE id = v_row.posting_line_id;
        IF v_orig.credit_account_id <> v_pool_id THEN
          CONTINUE;
        END IF;
        v_orig_reason := v_orig.reason;

        v_provisional := v_orig.amount / v_row.qty;
        v_variance    := (v_final_avg - v_provisional) * v_row.qty;

        -- acct-7eo: detect mixed-method rm_issue_to_wo. Source pool is
        -- wac_periodic (we're walking it). Destination pool's SKU may
        -- not be wac_periodic — that's the mixed shape.
        v_mixed := FALSE;
        IF v_orig_reason = 'rm_issue_to_wo' THEN
          SELECT s.cost_method::TEXT INTO v_dest_method
            FROM accounts a
            JOIN skus s ON s.id = a.sku_id
           WHERE a.id = v_orig.debit_account_id;
          IF v_dest_method IS DISTINCT FROM 'wac_periodic' THEN
            v_mixed := TRUE;
          END IF;
        END IF;

        IF v_mixed THEN
          IF v_variance = 0 THEN
            UPDATE posting_lines_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = 0,
                   variance_posting_line_id = NULL
             WHERE posting_line_id = v_row.posting_line_id;
            v_count := v_count + 1;
            CONTINUE;
          END IF;

          SELECT id INTO v_var_acct FROM accounts
           WHERE kind = 'variance_material_mixed'
             AND ledger_kind = 'value'
             AND currency = v_pool_acct.currency
             AND NOT is_closed;
          IF v_var_acct IS NULL THEN
            RAISE EXCEPTION
              'wac_periodic_close: no variance_material_mixed(value, ccy=%) '
              'account configured (acct-7eo)',
              v_pool_acct.currency USING ERRCODE = 'P0010';
          END IF;

          v_orig_credit := v_orig.credit_account_id;
          IF v_variance > 0 THEN
            v_event_a := jsonb_build_object(
              'reason',            'cost_restate',
              'document_kind',     'wac_periodic_close_mixed',
              'document_id',       gen_random_uuid(),
              'debit_account_id',  v_var_acct,
              'credit_account_id', v_orig_credit,
              'amount',            v_variance,
              'business_date',     v_period_closes,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         '00000000-0000-0000-0000-000000000000'
            );
          ELSE
            v_event_a := jsonb_build_object(
              'reason',            'cost_restate',
              'document_kind',     'wac_periodic_close_mixed',
              'document_id',       gen_random_uuid(),
              'debit_account_id',  v_orig_credit,
              'credit_account_id', v_var_acct,
              'amount',            -v_variance,
              'business_date',     v_period_closes,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         '00000000-0000-0000-0000-000000000000'
            );
          END IF;
          v_batch := jsonb_build_array(v_event_a);
          PERFORM post_posting_lines(v_batch, TRUE);
          SELECT id INTO v_var_xfer_id
            FROM posting_lines
           WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;
          UPDATE posting_lines_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = v_variance,
                 variance_posting_line_id = v_var_xfer_id
           WHERE posting_line_id = v_row.posting_line_id;
          v_count := v_count + 1;
          CONTINUE;
        END IF;

        IF v_orig_reason IN ('op_move_v', 'rm_issue_to_wo') THEN
          UPDATE posting_lines_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = v_variance,
                 variance_posting_line_id = NULL
           WHERE posting_line_id = v_row.posting_line_id;
          v_count := v_count + 1;
          CONTINUE;
        END IF;

        IF v_variance = 0 THEN
          UPDATE posting_lines_provisional
             SET finalized_at = clock_timestamp(),
                 variance_amount = 0,
                 variance_posting_line_id = NULL
           WHERE posting_line_id = v_row.posting_line_id;
          v_count := v_count + 1;
          CONTINUE;
        END IF;

        SELECT id INTO v_var_acct
          FROM accounts
         WHERE kind = 'variance_wac_periodic'
           AND ledger_kind = 'value'
           AND currency = v_pool_acct.currency
           AND NOT is_closed;
        IF v_var_acct IS NULL THEN
          RAISE EXCEPTION
            'wac_periodic_close: no variance_wac_periodic(value, ccy=%) account configured',
            v_pool_acct.currency USING ERRCODE = 'P0010';
        END IF;

        v_orig_debit  := v_orig.debit_account_id;
        v_orig_credit := v_orig.credit_account_id;

        IF v_pool_acct.kind = 'inv_value_wip' THEN
          IF v_variance > 0 THEN
            v_event_a := jsonb_build_object(
              'reason',            'cost_restate',
              'document_kind',     'wac_periodic_close',
              'document_id',       gen_random_uuid(),
              'debit_account_id',  v_orig_debit,
              'credit_account_id', v_var_acct,
              'amount',            v_variance,
              'business_date',     v_period_closes,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         '00000000-0000-0000-0000-000000000000'
            );
          ELSE
            v_event_a := jsonb_build_object(
              'reason',            'cost_restate',
              'document_kind',     'wac_periodic_close',
              'document_id',       gen_random_uuid(),
              'debit_account_id',  v_var_acct,
              'credit_account_id', v_orig_debit,
              'amount',            -v_variance,
              'business_date',     v_period_closes,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         '00000000-0000-0000-0000-000000000000'
            );
          END IF;
          v_batch := jsonb_build_array(v_event_a);
        ELSE
          IF v_variance > 0 THEN
            v_event_a := jsonb_build_object(
              'reason',            'cost_restate',
              'document_kind',     'wac_periodic_close',
              'document_id',       gen_random_uuid(),
              'debit_account_id',  v_orig_debit,
              'credit_account_id', v_var_acct,
              'amount',            v_variance,
              'business_date',     v_period_closes,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         '00000000-0000-0000-0000-000000000000'
            );
            v_event_b := jsonb_build_object(
              'reason',            'cost_restate',
              'document_kind',     'wac_periodic_close',
              'document_id',       gen_random_uuid(),
              'debit_account_id',  v_var_acct,
              'credit_account_id', v_orig_credit,
              'amount',            v_variance,
              'business_date',     v_period_closes,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         '00000000-0000-0000-0000-000000000000'
            );
          ELSE
            v_event_a := jsonb_build_object(
              'reason',            'cost_restate',
              'document_kind',     'wac_periodic_close',
              'document_id',       gen_random_uuid(),
              'debit_account_id',  v_var_acct,
              'credit_account_id', v_orig_debit,
              'amount',            -v_variance,
              'business_date',     v_period_closes,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         '00000000-0000-0000-0000-000000000000'
            );
            v_event_b := jsonb_build_object(
              'reason',            'cost_restate',
              'document_kind',     'wac_periodic_close',
              'document_id',       gen_random_uuid(),
              'debit_account_id',  v_orig_credit,
              'credit_account_id', v_var_acct,
              'amount',            -v_variance,
              'business_date',     v_period_closes,
              'idempotency_key',   gen_random_uuid(),
              'posted_by',         '00000000-0000-0000-0000-000000000000'
            );
          END IF;
          v_batch := jsonb_build_array(v_event_a, v_event_b);
        END IF;

        PERFORM post_posting_lines(v_batch, TRUE);

        SELECT id INTO v_var_xfer_id
          FROM posting_lines
         WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;

        UPDATE posting_lines_provisional
           SET finalized_at = clock_timestamp(),
               variance_amount = v_variance,
               variance_posting_line_id = v_var_xfer_id
         WHERE posting_line_id = v_row.posting_line_id;
        v_count := v_count + 1;
      END LOOP;

      DELETE FROM _wac_pools WHERE pool_id = v_pool_id;
      v_processed := array_append(v_processed, v_pool_id);
    END LOOP;

    IF v_progress = 0 THEN
      SELECT string_agg(pool_id::TEXT, ', ' ORDER BY pool_id)
        INTO v_cycle_pools
        FROM _wac_pools;
      RAISE EXCEPTION
        'wac_periodic_pool_cycle: period % (id=%) has rework cycles in '
        'wac_periodic op_move_v / rm_issue_to_wo flow involving pools [%]; '
        'iterative-fixed-point handling deferred to acct-p7v-rework.',
        v_period_code, p_period_id, v_cycle_pools
        USING ERRCODE = 'P0036';
    END IF;
  END LOOP;

  RETURN v_count;
END;
$$;

-- ============================================================
-- wac_retroactive_close_hook
--
-- Topological pool walk + per-pool chronological replay with merged
-- value/qty stream. For inv_value_wip pools, the merged stream pairs
-- value-leg events with qty-leg events on the matching stock_wip
-- account, ordered by (business_date, doc_chrono, document_id,
-- sub_priority, id) where doc_chrono = MIN(posted_at) OVER (PARTITION
-- BY document_id) and sub_priority sorts qty-in BEFORE value BEFORE
-- qty-out so the recompute uses the right pre-decrement pool_qty.
--
-- Pre-period state for inv_value_wip pools: qty from stock_wip; value
-- from the value pool.
-- For raw/fg pools: both from the value pool (per-class signed sum).
--
-- Internal-chain reasons (op_move_v / rm_issue_to_wo): record variance
-- only; cost shift propagates via cache. Mixed-method (acct-7eo):
-- single-leg variance through variance_material_mixed at component pool.
--
-- Rework cycles → P0036.
-- ============================================================

CREATE OR REPLACE FUNCTION wac_retroactive_close_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_pool_id        BIGINT;
  v_processed      BIGINT[] := ARRAY[]::BIGINT[];
  v_remaining      INT;
  v_progress       INT;
  v_cycle_pools    TEXT;
  v_pool_acct      accounts%ROWTYPE;
  v_qty_pool_id    BIGINT;
  v_pool_value     BIGINT;
  v_pool_qty       BIGINT;
  v_event          RECORD;
  v_recomputed_avg BIGINT;
  v_recomputed_amt BIGINT;
  v_variance       BIGINT;
  v_var_acct       BIGINT;
  v_batch          JSONB;
  v_var_xfer_id    BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
  v_orig_reason    posting_line_reason;
  v_dest_method    TEXT;
  v_mixed          BOOLEAN;
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN
    RETURN 0;
  END IF;

  CREATE TEMP TABLE _wac_retro_pools (
    pool_id BIGINT PRIMARY KEY
  ) ON COMMIT DROP;

  CREATE TEMP TABLE _wac_retro_edges (
    predecessor BIGINT,
    successor   BIGINT,
    PRIMARY KEY (predecessor, successor)
  ) ON COMMIT DROP;

  INSERT INTO _wac_retro_pools (pool_id)
  SELECT DISTINCT t.credit_account_id
    FROM posting_lines_provisional p
    JOIN posting_lines t ON t.id = p.posting_line_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_retroactive'
     AND p.finalized_at IS NULL
  UNION
  SELECT DISTINCT t.debit_account_id
    FROM posting_lines_provisional p
    JOIN posting_lines t ON t.id = p.posting_line_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_retroactive'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  INSERT INTO _wac_retro_edges (predecessor, successor)
  SELECT DISTINCT t.credit_account_id, t.debit_account_id
    FROM posting_lines_provisional p
    JOIN posting_lines t ON t.id = p.posting_line_id
   WHERE p.period_id = p_period_id
     AND p.cost_method = 'wac_retroactive'
     AND p.finalized_at IS NULL
     AND t.reason IN ('op_move_v', 'rm_issue_to_wo')
  ON CONFLICT DO NOTHING;

  LOOP
    SELECT COUNT(*) INTO v_remaining FROM _wac_retro_pools;
    EXIT WHEN v_remaining = 0;

    v_progress := 0;
    FOR v_pool_id IN
      SELECT wp.pool_id
        FROM _wac_retro_pools wp
       WHERE NOT EXISTS (
         SELECT 1 FROM _wac_retro_edges e
          WHERE e.successor = wp.pool_id
            AND e.predecessor IN (SELECT pool_id FROM _wac_retro_pools)
       )
    LOOP
      v_progress := v_progress + 1;

      SELECT * INTO v_pool_acct FROM accounts WHERE id = v_pool_id;

      IF v_pool_acct.kind = 'inv_value_wip' THEN
        v_qty_pool_id := _post_posting_lines_lookup_qty_account(v_pool_acct);
        IF v_qty_pool_id IS NULL THEN
          RAISE EXCEPTION
            'wac_retroactive_close: cannot resolve stock_wip qty account '
            'for inv_value_wip pool % (sku=% op=%)',
            v_pool_id, v_pool_acct.sku_id, v_pool_acct.routing_op
            USING ERRCODE = 'P0010';
        END IF;
      ELSE
        v_qty_pool_id := v_pool_id;
      END IF;

      -- Pre-period state.
      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_pool_id THEN  t.amount
                               WHEN t.credit_account_id = v_pool_id THEN -t.amount END), 0)
        INTO v_pool_value
        FROM posting_lines t
       WHERE v_pool_id IN (t.debit_account_id, t.credit_account_id)
         AND t.business_date < v_period_opens;

      SELECT COALESCE(SUM(CASE WHEN t.debit_account_id  = v_qty_pool_id THEN  t.qty
                               WHEN t.credit_account_id = v_qty_pool_id THEN -t.qty END), 0)
        INTO v_pool_qty
        FROM posting_lines t
       WHERE v_qty_pool_id IN (t.debit_account_id, t.credit_account_id)
         AND t.qty IS NOT NULL
         AND t.business_date < v_period_opens;

      FOR v_event IN
        WITH value_events AS (
          SELECT t.id,
                 CASE
                   WHEN t.debit_account_id = v_pool_id
                        THEN t.amount + COALESCE(p_cache.variance_amount, 0)
                   ELSE t.amount
                 END AS adj_amount,
                 t.amount AS orig_amount,
                 t.qty,
                 t.debit_account_id,
                 t.credit_account_id,
                 t.business_date,
                 t.posted_at,
                 t.document_id,
                 t.reason,
                 (p_my.posting_line_id IS NOT NULL) AS is_prov,
                 1 AS sub_priority,
                 'value'::TEXT AS leg
            FROM posting_lines t
            LEFT JOIN posting_lines_provisional p_cache
                   ON p_cache.posting_line_id = t.id
                  AND p_cache.finalized_at IS NOT NULL
                  AND p_cache.variance_posting_line_id IS NULL
            LEFT JOIN posting_lines_provisional p_my
                   ON p_my.posting_line_id = t.id
                  AND p_my.cost_method = 'wac_retroactive'
                  AND p_my.finalized_at IS NULL
                  AND t.credit_account_id = v_pool_id
           WHERE v_pool_id IN (t.debit_account_id, t.credit_account_id)
             AND t.business_date BETWEEN v_period_opens AND v_period_closes
        ),
        qty_events AS (
          SELECT t.id,
                 t.amount AS adj_amount,
                 t.amount AS orig_amount,
                 t.qty,
                 t.debit_account_id,
                 t.credit_account_id,
                 t.business_date,
                 t.posted_at,
                 t.document_id,
                 t.reason,
                 FALSE AS is_prov,
                 CASE WHEN t.debit_account_id = v_qty_pool_id THEN 0 ELSE 2 END AS sub_priority,
                 'qty'::TEXT AS leg
            FROM posting_lines t
           WHERE v_pool_acct.kind = 'inv_value_wip'
             AND v_qty_pool_id <> v_pool_id
             AND v_qty_pool_id IN (t.debit_account_id, t.credit_account_id)
             AND t.qty IS NOT NULL
             AND t.business_date BETWEEN v_period_opens AND v_period_closes
        ),
        merged AS (
          SELECT * FROM value_events
          UNION ALL
          SELECT * FROM qty_events
        ),
        ordered AS (
          SELECT *,
                 MIN(posted_at) OVER (PARTITION BY document_id) AS doc_chrono
            FROM merged
        )
        SELECT * FROM ordered
        ORDER BY business_date, doc_chrono, document_id, sub_priority, id
      LOOP
        IF v_event.leg = 'qty' THEN
          IF v_event.debit_account_id = v_qty_pool_id THEN
            v_pool_qty := v_pool_qty + v_event.qty;
          ELSE
            v_pool_qty := v_pool_qty - v_event.qty;
          END IF;
          CONTINUE;
        END IF;

        IF v_event.debit_account_id = v_pool_id THEN
          v_pool_value := v_pool_value + v_event.adj_amount;
          IF v_pool_acct.kind <> 'inv_value_wip' AND v_event.qty IS NOT NULL THEN
            v_pool_qty := v_pool_qty + v_event.qty;
          END IF;
          CONTINUE;
        END IF;

        IF v_event.qty IS NULL THEN
          v_pool_value := v_pool_value - v_event.orig_amount;
          CONTINUE;
        END IF;

        IF v_pool_qty <= 0 THEN
          IF p_force_provisional AND v_event.is_prov THEN
            CONTINUE;
          END IF;
          RAISE EXCEPTION
            'wac_retroactive_replay_pool_empty: period % (id=%) pool kind=% sku=% '
            'loc=% op=% ccy=%: running qty went non-positive at depletion of posting_line %; '
            'this indicates the perpetual chain has an inconsistency (more depletions '
            'than receipts of valid age). Pass p_force_provisional=TRUE to skip this row.',
            v_period_code, p_period_id, v_pool_acct.kind, v_pool_acct.sku_id,
            v_pool_acct.location_id, v_pool_acct.routing_op, v_pool_acct.currency,
            v_event.id
            USING ERRCODE = 'P0006';
        END IF;

        v_recomputed_avg := v_pool_value / v_pool_qty;
        v_recomputed_amt := v_event.qty * v_recomputed_avg;

        IF v_event.is_prov THEN
          v_variance    := v_recomputed_amt - v_event.orig_amount;
          v_orig_reason := v_event.reason;

          v_mixed := FALSE;
          IF v_orig_reason = 'rm_issue_to_wo' THEN
            SELECT s.cost_method::TEXT INTO v_dest_method
              FROM accounts a
              JOIN skus s ON s.id = a.sku_id
             WHERE a.id = v_event.debit_account_id;
            IF v_dest_method IS DISTINCT FROM 'wac_retroactive' THEN
              v_mixed := TRUE;
            END IF;
          END IF;

          IF v_mixed THEN
            IF v_variance = 0 THEN
              UPDATE posting_lines_provisional
                 SET finalized_at = clock_timestamp(),
                     variance_amount = 0,
                     variance_posting_line_id = NULL
               WHERE posting_line_id = v_event.id;
              v_count := v_count + 1;
            ELSE
              SELECT id INTO v_var_acct FROM accounts
               WHERE kind = 'variance_material_mixed'
                 AND ledger_kind = 'value'
                 AND currency = v_pool_acct.currency
                 AND NOT is_closed;
              IF v_var_acct IS NULL THEN
                RAISE EXCEPTION
                  'wac_retroactive_close: no variance_material_mixed(value, ccy=%) '
                  'account configured (acct-7eo)',
                  v_pool_acct.currency USING ERRCODE = 'P0010';
              END IF;

              IF v_variance > 0 THEN
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close_mixed',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_var_acct,
                  'credit_account_id', v_event.credit_account_id,
                  'amount',            v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              ELSE
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close_mixed',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_event.credit_account_id,
                  'credit_account_id', v_var_acct,
                  'amount',            -v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              END IF;
              v_batch := jsonb_build_array(v_event_a);
              PERFORM post_posting_lines(v_batch, TRUE);
              SELECT id INTO v_var_xfer_id
                FROM posting_lines
               WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;
              UPDATE posting_lines_provisional
                 SET finalized_at = clock_timestamp(),
                     variance_amount = v_variance,
                     variance_posting_line_id = v_var_xfer_id
               WHERE posting_line_id = v_event.id;
              v_count := v_count + 1;
            END IF;
          ELSIF v_orig_reason IN ('op_move_v', 'rm_issue_to_wo') THEN
            UPDATE posting_lines_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = v_variance,
                   variance_posting_line_id = NULL
             WHERE posting_line_id = v_event.id;
            v_count := v_count + 1;
          ELSIF v_variance = 0 THEN
            UPDATE posting_lines_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = 0,
                   variance_posting_line_id = NULL
             WHERE posting_line_id = v_event.id;
            v_count := v_count + 1;
          ELSE
            SELECT id INTO v_var_acct FROM accounts
             WHERE kind = 'variance_wac_retroactive' AND ledger_kind = 'value'
               AND currency = v_pool_acct.currency AND NOT is_closed;
            IF v_var_acct IS NULL THEN
              RAISE EXCEPTION
                'wac_retroactive_close: no variance_wac_retroactive(value, ccy=%) account configured',
                v_pool_acct.currency USING ERRCODE = 'P0010';
            END IF;

            IF v_pool_acct.kind = 'inv_value_wip' THEN
              IF v_variance > 0 THEN
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_event.debit_account_id,
                  'credit_account_id', v_var_acct,
                  'amount',            v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              ELSE
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_var_acct,
                  'credit_account_id', v_event.debit_account_id,
                  'amount',            -v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              END IF;
              v_batch := jsonb_build_array(v_event_a);
            ELSE
              IF v_variance > 0 THEN
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_event.debit_account_id,
                  'credit_account_id', v_var_acct,
                  'amount',            v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
                v_event_b := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_var_acct,
                  'credit_account_id', v_pool_id,
                  'amount',            v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              ELSE
                v_event_a := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_var_acct,
                  'credit_account_id', v_event.debit_account_id,
                  'amount',            -v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
                v_event_b := jsonb_build_object(
                  'reason',            'cost_restate',
                  'document_kind',     'wac_retroactive_close',
                  'document_id',       gen_random_uuid(),
                  'debit_account_id',  v_pool_id,
                  'credit_account_id', v_var_acct,
                  'amount',            -v_variance,
                  'business_date',     v_period_closes,
                  'idempotency_key',   gen_random_uuid(),
                  'posted_by',         '00000000-0000-0000-0000-000000000000'
                );
              END IF;
              v_batch := jsonb_build_array(v_event_a, v_event_b);
            END IF;

            PERFORM post_posting_lines(v_batch, TRUE);

            SELECT id INTO v_var_xfer_id
              FROM posting_lines
             WHERE idempotency_key = (v_event_a->>'idempotency_key')::UUID;

            UPDATE posting_lines_provisional
               SET finalized_at = clock_timestamp(),
                   variance_amount = v_variance,
                   variance_posting_line_id = v_var_xfer_id
             WHERE posting_line_id = v_event.id;
            v_count := v_count + 1;
          END IF;

          v_pool_value := v_pool_value - v_recomputed_amt;
          IF v_pool_acct.kind <> 'inv_value_wip' THEN
            v_pool_qty := v_pool_qty - v_event.qty;
          END IF;
        ELSE
          v_pool_value := v_pool_value - v_event.orig_amount;
          IF v_pool_acct.kind <> 'inv_value_wip' THEN
            v_pool_qty := v_pool_qty - v_event.qty;
          END IF;
        END IF;
      END LOOP;

      DELETE FROM _wac_retro_pools WHERE pool_id = v_pool_id;
      v_processed := array_append(v_processed, v_pool_id);
    END LOOP;

    IF v_progress = 0 THEN
      SELECT string_agg(pool_id::TEXT, ', ' ORDER BY pool_id)
        INTO v_cycle_pools
        FROM _wac_retro_pools;
      RAISE EXCEPTION
        'wac_retroactive_pool_cycle: period % (id=%) has rework cycles in '
        'wac_retroactive op_move_v / rm_issue_to_wo flow involving pools [%]; '
        'iterative-fixed-point handling deferred to acct-p7v-rework.',
        v_period_code, p_period_id, v_cycle_pools
        USING ERRCODE = 'P0036';
    END IF;
  END LOOP;

  RETURN v_count;
END;
$$;

-- ============================================================
-- cost_adjust_retroactive_hook
--
-- Walks un-finalized inventory_cost_adjustments_retroactive queue rows
-- for the period; for each, walks every credit-side qty-bearing
-- depletion on the (sku, location, currency, class) pool whose
-- business_date falls in the period; computes provisional_unit_cost
-- = amount / qty and variance = qty × (target_avg - provisional);
-- if non-zero, posts a 2-transfer batch through
-- variance_cost_adjust_retroactive.
--
-- Variance transfers carry qty=NULL so subsequent runs filter them out
-- via `qty IS NOT NULL`.
-- ============================================================

CREATE OR REPLACE FUNCTION cost_adjust_retroactive_hook(
  p_period_id         BIGINT,
  p_force_provisional BOOLEAN DEFAULT FALSE
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_period_opens   DATE;
  v_period_closes  DATE;
  v_period_code    TEXT;
  v_count          BIGINT := 0;
  v_queue          RECORD;
  v_pool_id        BIGINT;
  v_value_kind     TEXT;
  v_var_acct       BIGINT;
  v_event          RECORD;
  v_prov_unit      BIGINT;
  v_variance       BIGINT;
  v_total_variance BIGINT;
  v_dep_count      BIGINT;
  v_event_a        JSONB;
  v_event_b        JSONB;
  v_batch          JSONB;
BEGIN
  SELECT opens_at, closes_at, code
    INTO v_period_opens, v_period_closes, v_period_code
    FROM periods WHERE id = p_period_id;
  IF NOT FOUND THEN RETURN 0; END IF;

  FOR v_queue IN
    SELECT id, sku_id, location_id, currency, inventory_class, target_avg
      FROM inventory_cost_adjustments_retroactive
     WHERE target_period_id = p_period_id
       AND finalized_at IS NULL
     ORDER BY posted_at, id
     FOR UPDATE
  LOOP
    v_value_kind := 'inv_value_' || v_queue.inventory_class;
    EXECUTE format(
      'SELECT id FROM accounts WHERE kind = %L AND sku_id = $1 AND '
      'location_id = $2 AND currency = $3 AND NOT is_closed',
      v_value_kind
    ) INTO v_pool_id USING v_queue.sku_id, v_queue.location_id, v_queue.currency;
    IF v_pool_id IS NULL THEN
      RAISE EXCEPTION
        'cost_adjust_retroactive: no open % for sku=% loc=% ccy=% (queue id=%)',
        v_value_kind, v_queue.sku_id, v_queue.location_id, v_queue.currency, v_queue.id
        USING ERRCODE = 'P0010';
    END IF;

    SELECT id INTO v_var_acct
      FROM accounts
     WHERE kind = 'variance_cost_adjust_retroactive'
       AND ledger_kind = 'value'
       AND currency = v_queue.currency
       AND NOT is_closed;
    IF v_var_acct IS NULL THEN
      RAISE EXCEPTION
        'cost_adjust_retroactive: no variance_cost_adjust_retroactive(value, ccy=%) '
        'account configured', v_queue.currency
        USING ERRCODE = 'P0010';
    END IF;

    v_total_variance := 0;
    v_dep_count      := 0;

    FOR v_event IN
      SELECT t.id, t.amount, t.qty, t.debit_account_id
        FROM posting_lines t
       WHERE t.credit_account_id = v_pool_id
         AND t.business_date BETWEEN v_period_opens AND v_period_closes
         AND t.qty IS NOT NULL
         AND t.qty > 0
       ORDER BY t.business_date, t.posted_at, t.id
    LOOP
      v_prov_unit := v_event.amount / v_event.qty;
      v_variance  := v_event.qty * (v_queue.target_avg - v_prov_unit);

      IF v_variance = 0 THEN
        v_dep_count := v_dep_count + 1;
        CONTINUE;
      END IF;

      IF v_variance > 0 THEN
        v_event_a := jsonb_build_object(
          'reason','cost_restate','document_kind','cost_adjust_retroactive_close',
          'document_id', v_queue.id,
          'debit_account_id', v_event.debit_account_id,
          'credit_account_id', v_var_acct,
          'amount', v_variance,
          'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by','00000000-0000-0000-0000-000000000000');
        v_event_b := jsonb_build_object(
          'reason','cost_restate','document_kind','cost_adjust_retroactive_close',
          'document_id', v_queue.id,
          'debit_account_id', v_var_acct,
          'credit_account_id', v_pool_id,
          'amount', v_variance,
          'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by','00000000-0000-0000-0000-000000000000');
      ELSE
        v_event_a := jsonb_build_object(
          'reason','cost_restate','document_kind','cost_adjust_retroactive_close',
          'document_id', v_queue.id,
          'debit_account_id', v_var_acct,
          'credit_account_id', v_event.debit_account_id,
          'amount', -v_variance,
          'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by','00000000-0000-0000-0000-000000000000');
        v_event_b := jsonb_build_object(
          'reason','cost_restate','document_kind','cost_adjust_retroactive_close',
          'document_id', v_queue.id,
          'debit_account_id', v_pool_id,
          'credit_account_id', v_var_acct,
          'amount', -v_variance,
          'business_date', v_period_closes,
          'idempotency_key', gen_random_uuid(),
          'posted_by','00000000-0000-0000-0000-000000000000');
      END IF;

      v_batch := jsonb_build_array(v_event_a, v_event_b);
      PERFORM post_posting_lines(v_batch, FALSE);

      v_total_variance := v_total_variance + v_variance;
      v_dep_count      := v_dep_count + 1;
    END LOOP;

    UPDATE inventory_cost_adjustments_retroactive
       SET finalized_at    = clock_timestamp(),
           finalized_count = v_dep_count,
           total_variance  = v_total_variance
     WHERE id = v_queue.id;

    v_count := v_count + 1;
  END LOOP;

  RETURN v_count;
END;
$$;
