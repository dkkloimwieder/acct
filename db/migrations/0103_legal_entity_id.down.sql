-- acct-ewhs — revert legal_entity_id column

ALTER TABLE accounts DROP COLUMN legal_entity_id;
ALTER TABLE transfers DROP COLUMN legal_entity_id;
