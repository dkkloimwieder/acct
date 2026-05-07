-- acct-ow8 — rollback of A5 (bom_lines + self-reference trigger).

DROP TABLE IF EXISTS bom_lines;
DROP FUNCTION IF EXISTS _bom_line_self_reference_guard();
