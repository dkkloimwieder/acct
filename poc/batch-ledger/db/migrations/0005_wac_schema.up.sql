-- acct-4dg2 (P4 of acct-qdp5 PoC).
--
-- WAC perpetual schema additions. Two new account kinds and qty tracking:
--
--   inv_value_raw / inv_value_fg — value pools whose unit_cost is recomputed
--     per-batch from running value/qty.
--
--   accounts.qty — per-pool quantity tracker; for inv_value_* accounts only,
--     SUM(qty over inflows) - SUM(qty over outflows). For PoC simplicity this
--     is a mutable column on accounts (vs acct's R1 'SUM over posting_lines.qty'
--     pattern). The R1 invariant is preserved BY CONSTRUCTION in the batch
--     apply phase: the running balance map maintains qty exactly.
--
--   posting_lines.qty — per-row qty (signed by debit/credit on the pool side),
--     populated for WAC envelopes, NULL for plain transfers.

ALTER TYPE account_kind ADD VALUE 'inv_value_raw';
ALTER TYPE account_kind ADD VALUE 'inv_value_fg';

ALTER TABLE accounts      ADD COLUMN qty BIGINT NOT NULL DEFAULT 0;
ALTER TABLE posting_lines ADD COLUMN qty BIGINT NULL;
