-- Legal entities: row table for the legal_entity_id partition.
--
-- Promoted from archive mig 0103 (acct-ewhs), where legal_entity_id was
-- a SMALLINT column with DEFAULT 1 added to accounts + transfers as a
-- prelaid foundation. Now a real entity with FK references.
--
-- Future multi-entity work (acct-3gzh) extends this with additional
-- rows per legal entity. For the single-entity baseline, only id=1
-- exists; functional_currency 'USD' per acct-wb75 Q1 resolution.

CREATE TABLE legal_entities (
  id                  SMALLINT PRIMARY KEY CHECK (id > 0),
  name                VARCHAR(128) NOT NULL,
  functional_currency CHAR(3) NOT NULL DEFAULT 'USD',
  created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO legal_entities (id, name, functional_currency) VALUES
  (1, 'Default Entity', 'USD');

COMMENT ON TABLE legal_entities IS
  'Legal-entity reference table. Each accounts/posting_lines row carries '
  'a legal_entity_id FK back to here. Single-entity baseline ships with '
  'id=1 only; multi-entity expansion is acct-3gzh follow-up.';
