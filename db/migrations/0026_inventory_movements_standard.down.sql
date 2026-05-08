-- Best-effort down (project convention).
--
-- Restore _post_posting_lines_apply_event to mig 0024 baseline (without
-- the D-block) and drop the helper. mig 0024.down would be the right
-- place to fully unwind, but since migrations are sequential and Phase
-- 0 has no production data, this down just drops the helper; reverting
-- the apply_event extension is best done via wipe + reapply.

DROP FUNCTION IF EXISTS _inventory_movement_event_type(posting_line_reason, NUMERIC);
