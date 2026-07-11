-- Postgres cannot remove enum labels; the three cost_adjustment labels remain
-- (harmless while unreferenced — the up's ADD VALUE IF NOT EXISTS re-applies
-- cleanly over them).
DROP SEQUENCE cost_adjustment_id_seq;
