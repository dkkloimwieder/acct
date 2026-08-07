DROP TRIGGER cost_settlement_period_guard ON cost_settlement;
DROP FUNCTION period_guard_cost_settlement();
DROP TRIGGER trx_line_period_guard ON trx_line;
DROP FUNCTION period_guard_trx_line();
