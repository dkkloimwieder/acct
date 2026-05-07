-- Append-only enforcement for posting_lines. Trigger blocks UPDATE
-- and DELETE; the only legitimate way to add new rows is via
-- post_posting_lines (defined in 0014).
--
-- Renamed from fn_block_transfer_modifications (acct-NEW1 helper
-- discipline pass — drops the archaic `fn_` prefix). Trigger renamed
-- to trg_posting_lines_append_only.

CREATE OR REPLACE FUNCTION block_posting_line_modifications()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'posting_lines are append-only; UPDATE/DELETE rejected'
    USING ERRCODE = 'P9999';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_posting_lines_append_only
  BEFORE UPDATE OR DELETE ON posting_lines
  FOR EACH ROW EXECUTE FUNCTION block_posting_line_modifications();
