-- Phase B1 of the posting-lines convergence plan
-- (research/posting-lines-convergence-plan.md §4.B.B1; original
-- archive mig 0104, acct-wb75.1.1).
--
-- Extension table for posting_lines that carries the four new fields
-- the proposal contributes that we don't already have first-class on
-- the base table:
--
--   - reverses_posting_line_id  — pointer to the original posting_line
--                                 this one reverses (forward-only;
--                                 historical reversals don't carry it
--                                 because posting_lines is append-only
--                                 and reversals are new rows).
--   - parent_document_id        — nested-document linkage (e.g., a
--                                 wo_event under a work_order, where
--                                 posting_lines.document_id =
--                                 wo_event.id and parent_document_id =
--                                 work_order.id).
--   - intercompany_pair_id      — correlates the two postings of an
--                                 intercompany saga (acct-w1v3, future).
--   - created_by_process        — pg_proc name of the document wrapper
--                                 that called post_posting_lines (e.g.,
--                                 'post_po_receipt', 'post_wo_complete').
--                                 Audit-trail field; not load-bearing.
--
-- The proposal's source_doc_type / source_doc_id / source_doc_line_id
-- fields are already first-class on posting_lines (document_kind /
-- document_id / document_line_id) per the row-per-pair preservation
-- (acct-1584); this extension adds ONLY the four new fields to avoid
-- fragmenting the source-link encoding.
--
-- 1:1 join with posting_lines via PK = posting_line_id. Most rows have
-- NO extension row — the four fields apply to a minority of events:
-- reversals, nested-doc cases, intercompany sagas, and any caller that
-- opts into the audit field. Pure-NULL extension rows are forbidden by
-- CHECK; the dispatcher (_post_posting_lines_apply_event in 0014)
-- skips the INSERT when all four fields are NULL on the event JSONB.
--
-- Forward references in 0014 (_post_posting_lines_apply_event writes
-- to posting_line_sources) resolve via plpgsql late-binding — table
-- existence is checked at execution time, not at function-creation
-- time, so 0014 → 0019 ordering is fine.

CREATE TABLE posting_line_sources (
  posting_line_id           BIGINT PRIMARY KEY REFERENCES posting_lines(id),
  reverses_posting_line_id  BIGINT REFERENCES posting_lines(id),
  parent_document_id        UUID,
  intercompany_pair_id      UUID,
  created_by_process        VARCHAR(64),
  created_at                TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

  CONSTRAINT posting_line_sources_not_all_null CHECK (
    reverses_posting_line_id IS NOT NULL
    OR parent_document_id IS NOT NULL
    OR intercompany_pair_id IS NOT NULL
    OR created_by_process IS NOT NULL
  ),

  CONSTRAINT posting_line_sources_no_self_reversal CHECK (
    reverses_posting_line_id IS NULL
    OR reverses_posting_line_id <> posting_line_id
  )
);

CREATE INDEX posting_line_sources_reverses
  ON posting_line_sources (reverses_posting_line_id)
  WHERE reverses_posting_line_id IS NOT NULL;

CREATE INDEX posting_line_sources_parent_doc
  ON posting_line_sources (parent_document_id)
  WHERE parent_document_id IS NOT NULL;

CREATE INDEX posting_line_sources_intercompany
  ON posting_line_sources (intercompany_pair_id)
  WHERE intercompany_pair_id IS NOT NULL;

COMMENT ON TABLE posting_line_sources IS
  'Phase B1 extension table for posting_lines. Adds reversal pointer, '
  'parent-document linkage, intercompany-pair correlation, and '
  'process-name audit. The proposal''s source_doc_type / _id / _line_id '
  'fields are already first-class on posting_lines (document_kind / '
  'document_id / document_line_id) per the row-per-pair preservation '
  '(acct-1584); this extension adds only the four new fields. '
  'Extension row exists only when any of the four is non-NULL — most '
  'posting_lines have no extension row.';
