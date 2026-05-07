CREATE OR REPLACE FUNCTION fn_block_transfer_modifications()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'transfers are append-only; UPDATE/DELETE rejected'
    USING ERRCODE = 'P9999';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_transfers_append_only
  BEFORE UPDATE OR DELETE ON transfers
  FOR EACH ROW EXECUTE FUNCTION fn_block_transfer_modifications();
