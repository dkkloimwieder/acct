-- acct-chzx — add posting_layer column to transfers
--
-- Future-proofing for IFRS / local-GAAP / tax-basis / management-adjustment
-- tagging without a transfers rewrite later. Default 1 (= Current layer)
-- preserves all existing single-ledger behavior; every existing transfer
-- becomes layer=1 transparently. Bitmask convention per the proposal §2.4.1
-- and D365's posting-layer model:
--
--   bit 0 (value 1)   = Current     (the operational / leading-ledger layer)
--   bit 1 (value 2)   = Operations  (operational reporting variant)
--   bit 2 (value 4)   = Tax         (tax-basis adjustments)
--   bits 3..9         = Custom 1..7 (values 8, 16, 32, 64, 128, 256, 512)
--
-- A single transfer participates in ONE OR MORE layers via its bitmask
-- (e.g., 5 = Current+Tax). This generalizes D365's single-layer-per-row
-- model; a tax-only adjustment posts with posting_layer=4 and is excluded
-- from Current-layer reports. Reporting filters via WHERE posting_layer & N != 0.
--
-- SMALLINT (2 bytes) supports up to 15 distinct layer bits — well past the
-- D365 ten-layer convention. Full bitmask use is forward-compatible; current
-- callers continue to use the default value 1 with no change.
--
-- Per research/architecture-synthesis.md §5.4 (Multi-ledger / posting-layer
-- comparison), §6.3 (gap-action), §9.A1 (free-and-clear additive phase),
-- §10.1 (issue catalog).
--
-- Scope is intentionally narrow: transfers only. The acct-chzx issue
-- originally proposed adding the column to accounts as well, but per the
-- proposal §2.3 and D365 actual product, posting_layer is a per-event tag,
-- not a per-account property. accounts.allowed_layers_mask is a different
-- concept (validation hint that an account should only be used in certain
-- layers) with no concrete use case yet.

ALTER TABLE transfers
  ADD COLUMN posting_layer SMALLINT NOT NULL DEFAULT 1
    CHECK (posting_layer > 0);

-- Partial index excluding the dominant value. A full-table B-tree on
-- posting_layer would be dominated by posting_layer=1 (the bulk) and useless
-- for selective filtering. The partial index serves non-default-layer
-- reporting (Tax-only, Operations-only, Custom-layer queries) without
-- bloat. Current-layer queries scan the bulk and don't need the index.
CREATE INDEX transfers_non_default_layer
  ON transfers (business_date, posting_layer)
  WHERE posting_layer != 1;
