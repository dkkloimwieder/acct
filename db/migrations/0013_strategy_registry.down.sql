DROP FUNCTION IF EXISTS _compute_amount_wac_retroactive_outbound(JSONB, accounts, accounts, INT);
DROP FUNCTION IF EXISTS _compute_amount_wac_periodic_outbound(JSONB, accounts, accounts, INT);
DROP FUNCTION IF EXISTS _compute_amount_wac_perpetual_outbound(JSONB, accounts, accounts, INT);
DROP FUNCTION IF EXISTS _compute_amount_standard_outbound(JSONB, accounts, accounts, INT);
DROP TABLE IF EXISTS cost_method_strategies;
