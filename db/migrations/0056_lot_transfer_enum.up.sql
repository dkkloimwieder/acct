-- ============================================================
-- Phase E2 lot follow-up — enum prelude for lot_transfer (acct-fzzw).
--
-- PG forbids using a newly-added enum value in the same transaction
-- as the ALTER TYPE that added it. The wrapper / apply_event work
-- in mig 0057 references 'lot_transfer' both via TEXT->enum cast
-- (wrapper builds JSONB events) and via direct enum comparison in
-- _post_posting_lines_apply_event. This mig is the thin "enum lift"
-- that runs in its own transaction so 0057 can proceed.
--
-- Same split pattern as mig 0045 (lot_fifo enum prelude for 0046).
-- ============================================================

ALTER TYPE posting_line_reason ADD VALUE IF NOT EXISTS 'lot_transfer';
