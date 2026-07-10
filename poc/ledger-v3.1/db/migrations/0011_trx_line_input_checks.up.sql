-- acct-0at4.3 (FEEDBACK #19) — input CHECKs on trx_line. The schema is the
-- authority for these invariants; ledger-core enforced them application-side
-- only, so a future entry point inserting trx_line directly could violate them.
--
-- qty <> 0: §3.7 dispatches receipt vs depletion on the SIGN of qty, so a
-- zero-qty line has no defined direction.
--
-- unit_cost >= 0: an asserted receipt cost is non-negative. Zero is permitted —
-- depletion lines carry unit_cost = 0 (the applied cost is derived from the pool,
-- not the line), and zero-cost receipts (sample / consigned goods) are valid.
ALTER TABLE trx_line
    ADD CONSTRAINT trx_line_qty_nonzero CHECK (qty <> 0);
ALTER TABLE trx_line
    ADD CONSTRAINT trx_line_unit_cost_nonneg CHECK (unit_cost >= 0);
