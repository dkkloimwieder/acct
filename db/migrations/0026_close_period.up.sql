-- acct-v51 / acct-s6n.2 — Phase 1 period-close orchestration.
--
-- Builds on the schema from migration 0025 (acct-4mt). Adds:
--   * close_period(...)              — orchestrates a period close
--   * wac_periodic_close_hook(...)   — stub (Epic B / acct-qfj fills body)
--   * wac_retroactive_close_hook(.)  — stub (Epic C / acct-9tw fills body)
--   * cost_adjust_retroactive_hook(.) — stub (Epic E / acct-og1 fills body)
--
-- Sequencing inside close_period:
--
--   1. Validate the period (exists AND not already closed) — P0014.
--   2. Run all three close-hooks. Each posts whatever variance
--      transfers its workflow needs INTO the period being closed.
--      This MUST happen before periods.closed_at is set, otherwise
--      post_transfers' P0005 gate would fire on the hooks' own
--      variance postings.
--      For s6n the hooks are stubs returning 0; they touch nothing.
--   3. Provisional gate (P0015): refuse if any transfers_provisional
--      row in this period still has finalized_at IS NULL, unless
--      p_force_provisional = TRUE.
--   4. Reconciliation gate (P0016): run_daily_reconciliation()
--      returns the count of alerts it just inserted; refuse close
--      if > 0, unless p_force_recon = TRUE.
--   5. UPDATE periods SET closed_at, closed_by.
--   6. RETURN a JSONB summary the caller can persist for audit.
--
-- Force flags are split (provisional vs recon) by deliberate design —
-- the two gates protect against very different failure modes (incomplete
-- workflow vs corrupt ledger), and an operator might reasonably want to
-- bypass one without bypassing the other. (acct-s6n design memo, 2026-05-01.)
--
-- Parameter order note:
--   PG requires that defaulted parameters follow non-defaulted ones,
--   so the signature is (p_period_id, p_actor, p_force_*, p_force_*).
--   The behavioral spec listed p_actor last for narrative reasons —
--   that's a doc-time ordering, not the SQL signature.
--
-- New error codes (documented in db/README.md by acct-fot / s6n.4):
--   P0014 — period_close_invalid       (period missing OR already closed)
--   P0015 — period_close_provisional   (un-finalized rows; gate)
--   P0016 — period_close_reconciliation (recon alerts; gate)
--
-- Hook contract (s6n stubs):
--   <name>(p_period_id BIGINT) RETURNS BIGINT  -- count finalized, 0 today.
-- Epics B / C / E may extend the signature when they replace the body
-- (e.g., to thread p_actor through for variance posted_by). That's
-- additive — close_period's call sites get updated in the same migration.

CREATE OR REPLACE FUNCTION wac_periodic_close_hook(p_period_id BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS $$
BEGIN
  -- Stub. Epic B (acct-qfj / wac_periodic) replaces this body with the
  -- single-pool periodic-WAC re-pricing pass.
  RETURN 0;
END;
$$;

CREATE OR REPLACE FUNCTION wac_retroactive_close_hook(p_period_id BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS $$
BEGIN
  -- Stub. Epic C (acct-9tw / wac_retroactive) replaces this body with
  -- the late-data reconciliation pass.
  RETURN 0;
END;
$$;

CREATE OR REPLACE FUNCTION cost_adjust_retroactive_hook(p_period_id BIGINT)
RETURNS BIGINT LANGUAGE plpgsql AS $$
BEGIN
  -- Stub. Epic E (acct-og1 / cost_adjustment retroactive) replaces this
  -- body with the retroactive cost-adjustment finalization pass.
  RETURN 0;
END;
$$;

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
  -- Step 1: validate period. FOR UPDATE serializes concurrent
  -- close_period() callers — two parallel attempts on the same period
  -- queue, the first commits, the second re-reads closed_at and
  -- raises P0014. post_transfers reads closed_at without locking, so
  -- the lock here doesn't impede normal posting.
  SELECT code, closed_at INTO v_period_code, v_already_closed
    FROM periods WHERE id = p_period_id FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'period_close_invalid: period id=% not found', p_period_id
      USING ERRCODE = 'P0014';
  END IF;

  IF v_already_closed IS NOT NULL THEN
    RAISE EXCEPTION
      'period_close_invalid: period % (id=%) already closed at %',
      v_period_code, p_period_id, v_already_closed
      USING ERRCODE = 'P0014';
  END IF;

  -- Step 2: run hooks. They must post any variance transfers they
  -- generate INTO this period BEFORE step 5 sets closed_at — otherwise
  -- post_transfers' P0005 gate would fire on the variance postings
  -- themselves. Stubs return 0.
  v_wac_period_count     := wac_periodic_close_hook(p_period_id);
  v_wac_retro_count      := wac_retroactive_close_hook(p_period_id);
  v_cost_adj_retro_count := cost_adjust_retroactive_hook(p_period_id);
  v_finalized_count      := v_wac_period_count
                          + v_wac_retro_count
                          + v_cost_adj_retro_count;

  -- Step 3: provisional gate.
  SELECT COUNT(*) INTO v_unfinalized_remaining
    FROM transfers_provisional
   WHERE period_id = p_period_id
     AND finalized_at IS NULL;

  IF v_unfinalized_remaining > 0 AND NOT p_force_provisional THEN
    RAISE EXCEPTION
      'period_close_provisional: % un-finalized provisional rows '
      'remain in period % (id=%); pass p_force_provisional=TRUE to override',
      v_unfinalized_remaining, v_period_code, p_period_id
      USING ERRCODE = 'P0015';
  END IF;

  -- Step 4: reconciliation gate.
  v_alerts := run_daily_reconciliation();

  IF v_alerts > 0 AND NOT p_force_recon THEN
    RAISE EXCEPTION
      'period_close_reconciliation: % new reconciliation alert(s) raised '
      'while closing period % (id=%); pass p_force_recon=TRUE to override',
      v_alerts, v_period_code, p_period_id
      USING ERRCODE = 'P0016';
  END IF;

  -- Step 5: stamp the close.
  v_now := clock_timestamp();
  UPDATE periods
     SET closed_at = v_now,
         closed_by = p_actor
   WHERE id = p_period_id;

  -- Step 6: audit summary.
  RETURN jsonb_build_object(
    'period_id',              p_period_id,
    'period_code',            v_period_code,
    'closed_at',              v_now,
    'closed_by',              p_actor,
    'finalized_count',        v_finalized_count,
    'hook_results',           jsonb_build_object(
      'wac_periodic',            v_wac_period_count,
      'wac_retroactive',         v_wac_retro_count,
      'cost_adjust_retroactive', v_cost_adj_retro_count
    ),
    'unfinalized_remaining',  v_unfinalized_remaining,
    'alerts',                 v_alerts,
    'forced',                 jsonb_build_object(
      'provisional',             p_force_provisional,
      'recon',                   p_force_recon
    )
  );
END;
$$;
