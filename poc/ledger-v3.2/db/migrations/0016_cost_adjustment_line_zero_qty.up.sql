-- The close sweep (recalc-e §5) posts a pool's settlement residue as a pure
-- value adjustment: a cost_adjustment_line with qty = 0 whose posting_line
-- carries the residue amount against the variance account. The qty <> 0 CHECK
-- exists because hot-path dispatch keys on the SIGN of qty; cost_adjustment_line
-- rows are the recalc/close engine's own GL output — excluded from replay,
-- gauges, and dispatch — so a zero-qty row is well-defined for them and only
-- them.
ALTER TABLE trx_line DROP CONSTRAINT trx_line_qty_nonzero;
ALTER TABLE trx_line ADD CONSTRAINT trx_line_qty_nonzero
    CHECK (qty <> 0 OR line_type = 'cost_adjustment_line');
