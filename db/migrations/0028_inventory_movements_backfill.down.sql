-- Best-effort down (project convention).
--
-- A pure data migration; reversal would require knowing which rows
-- were inserted by THIS migration vs. which by D2/D3 dispatcher
-- writes that ran on later posting_lines. There's no marker to
-- distinguish. In practice: wipe + reapply for a clean down.

DROP FUNCTION IF EXISTS _backfill_inventory_movements();
