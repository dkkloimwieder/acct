-- Best-effort down (project convention; PG has no DROP VALUE on enums).
-- 'lot_fifo' stays in the cost_method enum after revert. Harmless if
-- unreferenced.

SELECT 1;
