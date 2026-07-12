DROP TRIGGER ledger_inbox_backpressure ON ledger_inbox;
DROP FUNCTION ledger_inbox_backpressure_gate();
DROP TABLE recalc_backpressure;
DROP TABLE recalc_backlog;
DROP TRIGGER recalc_backpressure_config_keep ON recalc_backpressure_config;
DROP FUNCTION recalc_backpressure_config_guard();
DROP TABLE recalc_backpressure_config;
