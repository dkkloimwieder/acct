-- acct-0kl — drop the consumption_policy gate trigger.

DROP TRIGGER IF EXISTS wo_events_check_consumption_policy ON wo_events;
DROP FUNCTION IF EXISTS _wo_events_check_consumption_policy();
