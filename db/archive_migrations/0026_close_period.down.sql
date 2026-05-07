-- Revert acct-v51: drop close_period() and the three close-hooks.

DROP FUNCTION IF EXISTS close_period(BIGINT, UUID, BOOLEAN, BOOLEAN);
DROP FUNCTION IF EXISTS wac_periodic_close_hook(BIGINT);
DROP FUNCTION IF EXISTS wac_retroactive_close_hook(BIGINT);
DROP FUNCTION IF EXISTS cost_adjust_retroactive_hook(BIGINT);
