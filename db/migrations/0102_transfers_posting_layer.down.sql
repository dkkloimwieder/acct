-- acct-chzx — revert posting_layer column

DROP INDEX IF EXISTS transfers_non_default_layer;

ALTER TABLE transfers DROP COLUMN posting_layer;
