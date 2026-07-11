-- recalc-d §2 — the recalc engine's transaction taxonomy. Recalc posts its
-- authoritative valuation as ordinary ledger rows (trx / trx_line /
-- posting_line), so it needs its own labels:
--
--   trx_type 'cost_adjustment'            — the system-generated recalc trx
--                                           wrapping a pass's cost corrections
--   line_type 'cost_adjustment_line'      — the per-depletion correction line
--   posting_event_type 'cost_adjustment'  — DISTINCT from 'variance' (D7
--     resolved): 'variance' carries the hot-path STD purchase-price variance;
--     a separate label keeps deferred-recalc GL movement auditably separable
--     from PPV without disentangling reports.
--
-- IF NOT EXISTS keeps re-up idempotent across down/up cycles (the down cannot
-- remove enum labels).

ALTER TYPE trx_type ADD VALUE IF NOT EXISTS 'cost_adjustment';
ALTER TYPE line_type ADD VALUE IF NOT EXISTS 'cost_adjustment_line';
ALTER TYPE posting_event_type ADD VALUE IF NOT EXISTS 'cost_adjustment';

-- Supplies trx.source_id for adjustment trxs: system-generated, they still must
-- satisfy the UNIQUE (trx_type, source_id) idempotency key, and
-- ('cost_adjustment', <seq value>) is the natural idempotency handle for a
-- recalc pass itself.
CREATE SEQUENCE cost_adjustment_id_seq;
