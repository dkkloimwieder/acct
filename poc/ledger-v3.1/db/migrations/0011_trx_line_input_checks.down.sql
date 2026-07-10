ALTER TABLE trx_line DROP CONSTRAINT IF EXISTS trx_line_unit_cost_nonneg;
ALTER TABLE trx_line DROP CONSTRAINT IF EXISTS trx_line_qty_nonzero;
