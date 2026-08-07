DROP TRIGGER accounting_period_transitions ON accounting_period;
DROP FUNCTION accounting_period_transition_guard();
DROP TRIGGER cost_layer_consumption_append_only ON cost_layer_consumption;
DROP TRIGGER cost_settlement_append_only ON cost_settlement;
DROP TRIGGER posting_line_append_only ON posting_line;
DROP TRIGGER trx_line_append_only ON trx_line;
DROP FUNCTION reject_mutation();
