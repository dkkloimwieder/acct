-- acct-ewhs — add legal_entity_id column to transfers and accounts
--
-- Zero-cost prelaid foundation for future multi-entity work (acct-3gzh).
-- Single-entity behavior preserved via DEFAULT 1; the column becomes a real
-- foreign key and RBAC anchor when acct-3gzh introduces a `legal_entities`
-- row table. Until then it's a future-proofing scaffold that costs nothing.
--
-- Why both tables (unlike acct-chzx which narrowed to transfers only):
--
--   - Accounts are scoped to a legal entity. SAP, Oracle Fusion, D365, and
--     NetSuite OneWorld all treat each entity as having its own chart of
--     accounts (with optional shared / consolidated COA via mapping). A
--     vendor pool, customer pool, AR account, AP account — each exists
--     within one entity.
--
--   - Transfers happen within a single legal entity. Cross-entity flows
--     are intercompany sagas — two postings, one per entity, correlated
--     via an intercompany_pair_id (proposal §2.3.4 / §4.8). One physical
--     transfer is never half-in-A-half-in-B.
--
-- Scoping legal_entity_id on transfers AND accounts gives the natural
-- entity-isolation invariant: a transfer's legal_entity_id must match the
-- legal_entity_id of both its debit_account and credit_account. That CHECK
-- gets added when acct-3gzh promotes legal_entity_id from "always 1" to a
-- real FK; until then, the trivially-true single-entity case holds.
--
-- SMALLINT (2 bytes) supports up to 32K entities — generous for any
-- realistic deployment.
--
-- Per research/architecture-synthesis.md §5.4 (Multi-ledger / posting-
-- layer + legal_entity_id discussion), §6.3 (gap-action: prelay foundation
-- before multi-entity work), §9.A2 (free-and-clear additive phase).

ALTER TABLE transfers
  ADD COLUMN legal_entity_id SMALLINT NOT NULL DEFAULT 1
    CHECK (legal_entity_id > 0);

ALTER TABLE accounts
  ADD COLUMN legal_entity_id SMALLINT NOT NULL DEFAULT 1
    CHECK (legal_entity_id > 0);

-- No index added at this stage. With a single-entity codebase, every row
-- has legal_entity_id=1 — an index would be useless. When acct-3gzh
-- introduces multi-entity, indexes follow naturally on (legal_entity_id,
-- ...) composites alongside RBAC scoping. Adding now would just be bloat.
