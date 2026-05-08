-- Best-effort down (project convention).

DROP TRIGGER IF EXISTS trg_emit_correction_movement_for_variance ON posting_lines;
DROP FUNCTION IF EXISTS _emit_correction_movement_for_variance_post();
