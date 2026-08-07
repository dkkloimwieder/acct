-- design-v3.1 §3.7 (v3.2) — table-driven posting-account resolution (acct-y08r).
--
-- One full account set per (sku_id, location_id). The ledger resolves debit/credit
-- (and the STD purchase-price-variance account) here instead of taking them per-line
-- from the caller, so callers no longer need to know the GL chart of accounts. Mirrors
-- how standard_cost is already hydrated.
--
-- Each operation's pair is stored in the RECEIPT (inventory-increase) direction:
-- debit = the pool's inventory side, credit = the contra. The cost engine uses the
-- pair as-is for receipts (qty > 0) and SWAPS it for depletions (qty < 0). line_type
-- selects the operation:
--   receipt     <- po_receipt_line
--   transfer    <- transfer_shipment_line / transfer_receipt_line
--   build       <- wo_output / wo_backflush
--   scrap       <- wo_scrap
--   adjustment  <- inv_adjustment_line / manual_adjustment_line
--   revaluation <- revaluation_line
--
-- A touched pool whose (sku_id, location_id) has no row here fails loud
-- (LedgerError::MissingPostingAccounts -> ereport ERROR); every line posts a journal
-- row, so a missing config always surfaces. Same posture as standard_cost.
--
-- sku_id / location_id are plain BIGINT (no FK), matching standard_cost and pool (§2.2).
-- Account columns FK account(id), matching posting_line (§2.3). variance_acct is
-- nullable: pools that are never STD never need it.

CREATE TABLE posting_account_map (
    sku_id              BIGINT NOT NULL,
    location_id         BIGINT NOT NULL,
    receipt_debit       BIGINT NOT NULL REFERENCES account(id),
    receipt_credit      BIGINT NOT NULL REFERENCES account(id),
    transfer_debit      BIGINT NOT NULL REFERENCES account(id),
    transfer_credit     BIGINT NOT NULL REFERENCES account(id),
    build_debit         BIGINT NOT NULL REFERENCES account(id),
    build_credit        BIGINT NOT NULL REFERENCES account(id),
    scrap_debit         BIGINT NOT NULL REFERENCES account(id),
    scrap_credit        BIGINT NOT NULL REFERENCES account(id),
    adjustment_debit    BIGINT NOT NULL REFERENCES account(id),
    adjustment_credit   BIGINT NOT NULL REFERENCES account(id),
    revaluation_debit   BIGINT NOT NULL REFERENCES account(id),
    revaluation_credit  BIGINT NOT NULL REFERENCES account(id),
    variance_acct       BIGINT REFERENCES account(id),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sku_id, location_id)
);
