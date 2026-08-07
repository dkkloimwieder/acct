-- design-v3.1 §3.7 — table-driven posting-account resolution.
--
-- One full account set per (sku_id, location_id). The ledger resolves
-- debit/credit (and the STD purchase-price-variance account) here instead of
-- taking them per-line from the caller, so callers send inventory facts only
-- (pool_id, line_type, qty, unit_cost — the v3.2 wire contract, design-v3.2
-- §4). Mirrors how standard_cost is hydrated.
--
-- Each operation's pair is stored in the RECEIPT (inventory-increase)
-- direction: debit = the pool's inventory side, credit = the contra. The cost
-- engine uses the pair as-is for receipts (qty > 0) and SWAPS it for depletions
-- (qty < 0). line_type selects the operation:
--   receipt     <- po_receipt_line
--   transfer    <- transfer_shipment_line / transfer_receipt_line
--   build       <- wo_output / wo_backflush
--   scrap       <- wo_scrap
--   adjustment  <- inv_adjustment_line / manual_adjustment_line
--   revaluation <- revaluation_line
--
-- A touched pool whose (sku_id, location_id) has no row here fails loud
-- (MissingPostingAccounts); a missing STD variance account fails loud
-- (MissingVarianceAccount). No caller-supplied account fallback.
--
-- sku_id / location_id are plain BIGINT (no FK), matching standard_cost and
-- pool (§2.2). Account columns FK account(id), matching posting_line (§2.3).
-- variance_acct is nullable: pools that are never STD never need it.

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
