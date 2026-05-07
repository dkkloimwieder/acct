-- acct-wb75.1.1 — Phase B1 of the convergence plan (acct-wb75).
--
-- Adds the `transfer_line_sources` extension table — the proposal's
-- `posting_line_sources` (research/ledger-architecture-proposal.md
-- §2.3.4), adapted to our row-per-pair `transfers` shape (acct-1584).
--
-- DESIGN NOTE — DIVERGENCE FROM PROPOSAL §2.3.4:
--
-- The proposal's `posting_line_sources` extension carries six fields:
--   source_doc_type, source_doc_id, source_doc_line_id,
--   reverses_posting_id, parent_document_id, intercompany_pair_id.
--
-- The first three are already first-class on our `transfers` table
-- (mig 0007: `document_kind TEXT`, `document_id UUID`,
-- `document_line_id UUID`). Re-adding them here would create
-- fragmentation — two parallel encodings of the same source link.
-- Per acct-1584 (row-per-pair preserved) and the convergence plan
-- §4.B.B1 ("convergence-additive, not migration-out"), this extension
-- adds ONLY the four fields the proposal contributes that we DO NOT
-- have today:
--
--   - reverses_transfer_id   — pointer to the original transfer this
--                              one reverses (forward-only; historical
--                              reversals don't have it because
--                              transfers is append-only and reversals
--                              are new transfers, not edits).
--   - parent_document_id     — nested-document linkage (e.g., a
--                              wo_event under a work_order, where
--                              transfers.document_id = wo_event.id and
--                              parent_document_id = work_order.id).
--   - intercompany_pair_id   — correlates the two postings of an
--                              intercompany saga (acct-w1v3, future).
--   - created_by_process     — pg_proc name of the document wrapper
--                              that called post_transfers (e.g.,
--                              'post_po_receipt', 'post_wo_complete').
--                              Audit-trail field; not load-bearing.
--
-- The eventual lookup-table conversion of `document_kind` (TEXT →
-- SMALLINT FK) tracks alongside acct-2thf (account_kind enum→row);
-- not in scope for B1.
--
-- BACKFILL: NONE. All four fields are forward-only. Historical
-- transfers (mig 0001-0103) get no extension row. Going forward, the
-- dispatcher (post_transfers / _post_transfers_apply_event) reads
-- these fields off the event JSONB and writes the extension row when
-- any are non-NULL. Pure-NULL extension rows are forbidden by CHECK.
--
-- 1:1 join with transfers via PK = transfer_id. Most transfers will
-- have NO extension row (the four fields apply to a minority of
-- events: reversals, nested-doc cases, intercompany sagas, and any
-- caller that opts into the audit field).

CREATE TABLE transfer_line_sources (
  transfer_id           BIGINT PRIMARY KEY REFERENCES transfers(id),
  reverses_transfer_id  BIGINT REFERENCES transfers(id),
  parent_document_id    UUID,
  intercompany_pair_id  UUID,
  created_by_process    VARCHAR(64),
  created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

  -- Pure-NULL extension row is a bug — caller passed all four fields
  -- as NULL and the dispatcher should have skipped the INSERT.
  CONSTRAINT transfer_line_sources_not_all_null CHECK (
    reverses_transfer_id IS NOT NULL
    OR parent_document_id IS NOT NULL
    OR intercompany_pair_id IS NOT NULL
    OR created_by_process IS NOT NULL
  ),

  -- A transfer cannot reverse itself.
  CONSTRAINT transfer_line_sources_no_self_reversal CHECK (
    reverses_transfer_id IS NULL OR reverses_transfer_id <> transfer_id
  )
);

CREATE INDEX transfer_line_sources_reverses
  ON transfer_line_sources (reverses_transfer_id)
  WHERE reverses_transfer_id IS NOT NULL;

CREATE INDEX transfer_line_sources_parent_doc
  ON transfer_line_sources (parent_document_id)
  WHERE parent_document_id IS NOT NULL;

CREATE INDEX transfer_line_sources_intercompany
  ON transfer_line_sources (intercompany_pair_id)
  WHERE intercompany_pair_id IS NOT NULL;

COMMENT ON TABLE transfer_line_sources IS
  'Phase B1 extension table for transfers (acct-wb75.1.1). Adds '
  'reversal pointer, parent-document linkage, intercompany-pair '
  'correlation, and process-name audit. The proposal''s '
  'source_doc_type / _id / _line_id fields are already first-class on '
  'transfers (document_kind / document_id / document_line_id) per the '
  'row-per-pair preservation in acct-1584; this extension adds only '
  'the four NEW fields the proposal contributes that we don''t '
  'already have. Extension row exists only when any of the four is '
  'non-NULL — most transfers have no extension row.';

-- Extend _post_transfers_apply_event to write the extension row
-- when any of the four fields is non-NULL on the event JSONB.
-- Signature unchanged from mig 0067; only the body is rewritten.
-- The new fields are read off p_event after the transfers INSERT.
--
-- The function body is otherwise identical to the mig 0067 (acct-7py)
-- definition: gates, period resolution, qty resolution, balance
-- update, transfer insert, and the wac_periodic / wac_retroactive
-- provisional-flag logic. Only the new tail block at the end is new.

CREATE OR REPLACE FUNCTION _post_transfers_apply_event(
  p_event           JSONB,
  p_idx             INT,
  p_amount          BIGINT,
  p_d_acct          accounts,
  p_c_acct          accounts,
  p_cost_method     cost_method,
  p_override_closed BOOLEAN
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_period_id      BIGINT;
  v_period_closed  TIMESTAMPTZ;
  v_business_date  DATE;
  v_qty_for_row    BIGINT;
  v_reason         transfer_reason;
  v_idem_key       UUID;
  v_new_id         BIGINT;
  v_event_qty      BIGINT;
  v_resolved_cm    cost_method;
  v_cost_sku       UUID;
  -- B1 extension fields.
  v_reverses_id    BIGINT;
  v_parent_doc     UUID;
  v_ic_pair        UUID;
  v_proc           VARCHAR;
BEGIN
  v_business_date := (p_event->>'business_date')::DATE;
  v_reason        := (p_event->>'reason')::transfer_reason;
  v_idem_key      := (p_event->>'idempotency_key')::UUID;

  IF p_d_acct.is_closed OR p_c_acct.is_closed THEN
    RAISE EXCEPTION 'account_closed: event index %', p_idx
      USING ERRCODE = 'P0001';
  END IF;
  IF p_d_acct.ledger_kind <> p_c_acct.ledger_kind THEN
    RAISE EXCEPTION 'ledger_mismatch: event index % (% vs %)',
                    p_idx, p_d_acct.ledger_kind, p_c_acct.ledger_kind
      USING ERRCODE = 'P0002';
  END IF;
  IF p_d_acct.ledger_kind = 'value'
     AND p_d_acct.currency <> p_c_acct.currency THEN
    RAISE EXCEPTION 'currency_mismatch: event index % (% vs %)',
                    p_idx, p_d_acct.currency, p_c_acct.currency
      USING ERRCODE = 'P0003';
  END IF;

  SELECT id, closed_at INTO v_period_id, v_period_closed
    FROM periods
   WHERE opens_at <= v_business_date AND closes_at >= v_business_date;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'period_missing: event index % business_date %',
                    p_idx, v_business_date USING ERRCODE = 'P0004';
  END IF;
  IF v_period_closed IS NOT NULL AND NOT p_override_closed THEN
    RAISE EXCEPTION 'period_closed: event index % business_date %',
                    p_idx, v_business_date USING ERRCODE = 'P0005';
  END IF;

  v_qty_for_row := (p_event->>'qty')::BIGINT;
  IF v_qty_for_row IS NULL
     AND p_d_acct.ledger_kind = 'qty'
     AND p_c_acct.ledger_kind = 'qty' THEN
    v_qty_for_row := p_amount;
  END IF;

  UPDATE accounts SET debits_total  = debits_total  + p_amount
    WHERE id = p_d_acct.id;
  UPDATE accounts SET credits_total = credits_total + p_amount
    WHERE id = p_c_acct.id;
  INSERT INTO transfers (
    reason, document_kind, document_id, document_line_id,
    debit_account_id, credit_account_id, amount, qty,
    routing_op, counterparty_id, period_id, business_date,
    idempotency_key, posted_by
  ) VALUES (
    v_reason, p_event->>'document_kind', (p_event->>'document_id')::UUID,
    (p_event->>'document_line_id')::UUID, p_d_acct.id, p_c_acct.id,
    p_amount, v_qty_for_row,
    (p_event->>'routing_op')::INT, (p_event->>'counterparty_id')::UUID,
    v_period_id, v_business_date, v_idem_key,
    (p_event->>'posted_by')::UUID
  ) RETURNING id INTO v_new_id;

  -- Provisional flag for wac_periodic / wac_retroactive depletions
  -- (mig 0067 / acct-7py — preserved verbatim).
  IF v_reason IN ('op_move','scrap','wo_complete','so_ship',
                  'op_move_v','scrap_v','wo_complete_v',
                  'rm_issue_to_wo')
     AND p_d_acct.ledger_kind = 'value' THEN
    v_resolved_cm := p_cost_method;
    IF v_resolved_cm IS NULL THEN
      v_cost_sku := COALESCE(p_c_acct.sku_id, p_d_acct.sku_id);
      IF v_cost_sku IS NOT NULL THEN
        SELECT cost_method INTO v_resolved_cm FROM skus WHERE id = v_cost_sku;
      END IF;
    END IF;
    IF v_resolved_cm IN ('wac_periodic', 'wac_retroactive') THEN
      v_event_qty := (p_event->>'qty')::BIGINT;
      INSERT INTO transfers_provisional (transfer_id, period_id, cost_method, qty)
      VALUES (v_new_id, v_period_id, v_resolved_cm, v_event_qty);
    END IF;
  END IF;

  -- B1 extension write (acct-wb75.1.1). Insert transfer_line_sources
  -- only when caller passed any of the four new fields. Pure-NULL
  -- extension rows are forbidden by CHECK on the table.
  v_reverses_id := (p_event->>'reverses_transfer_id')::BIGINT;
  v_parent_doc  := (p_event->>'parent_document_id')::UUID;
  v_ic_pair     := (p_event->>'intercompany_pair_id')::UUID;
  v_proc        := p_event->>'created_by_process';
  IF v_reverses_id IS NOT NULL
     OR v_parent_doc  IS NOT NULL
     OR v_ic_pair     IS NOT NULL
     OR v_proc        IS NOT NULL THEN
    INSERT INTO transfer_line_sources (
      transfer_id, reverses_transfer_id, parent_document_id,
      intercompany_pair_id, created_by_process
    ) VALUES (
      v_new_id, v_reverses_id, v_parent_doc, v_ic_pair, v_proc
    );
  END IF;

  RETURN v_new_id;
END;
$$;

COMMENT ON FUNCTION _post_transfers_apply_event(JSONB, INT, BIGINT, accounts, accounts, cost_method, BOOLEAN) IS
  'Single-event apply step. Phase B1 (acct-wb75.1.1): adds optional '
  'extension write to transfer_line_sources when caller passes any '
  'of reverses_transfer_id / parent_document_id / intercompany_pair_id '
  '/ created_by_process on the event JSONB. Tier 2 (acct-7py) '
  'behavior preserved: rm_issue_to_wo in flagged cost-event reasons; '
  'credit-first SKU resolution.';
