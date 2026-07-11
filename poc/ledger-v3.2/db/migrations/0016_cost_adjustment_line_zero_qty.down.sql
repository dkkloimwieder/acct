-- Best-effort: fails if zero-qty cost_adjustment_line rows exist (dev databases
-- only; delete them first or reset).
ALTER TABLE trx_line DROP CONSTRAINT trx_line_qty_nonzero;
ALTER TABLE trx_line ADD CONSTRAINT trx_line_qty_nonzero CHECK (qty <> 0);
