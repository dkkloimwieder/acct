-- acct-98e / BOM2 Phase A2 — engineering_change_orders table.
--
-- Foundation for the ECO-driven BOM revision workflow. The bom_headers
-- table (A4) has a nullable eco_id FK referencing this table; the
-- post_eco_approve workflow function (B12) walks bom_headers to phase
-- in approved revisions by stamping effective_at / obsolete_at.
--
-- Status workflow: draft → {approved, rejected} (terminal); approved
-- can later transition to superseded when a newer ECO replaces it.
-- The CHECK enforces that approved rows carry approved_by + approved_at
-- + effective_at; non-approved rows have those fields nullable.
--
-- For this refactor (epic acct-jg2): the table + columns are in scope.
-- The post_eco_approve workflow function is B12 — may ship in 0041
-- or punt to acct-ir7 (Phase 2 ECO-WORKFLOW); table is needed either
-- way so bom_headers can FK to it.

CREATE TABLE engineering_change_orders (
  id              BIGSERIAL PRIMARY KEY,
  code            TEXT UNIQUE NOT NULL,
  description     TEXT NOT NULL,
  status          TEXT NOT NULL DEFAULT 'draft'
                  CHECK (status IN ('draft', 'approved', 'rejected', 'superseded')),
  requested_by    UUID NOT NULL,
  requested_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  approved_by     UUID,
  approved_at     TIMESTAMPTZ,
  effective_at    TIMESTAMPTZ,
  rejected_reason TEXT,
  CHECK (
    (status = 'approved'
     AND approved_by IS NOT NULL
     AND approved_at IS NOT NULL
     AND effective_at IS NOT NULL
     AND rejected_reason IS NULL)
    OR
    (status = 'rejected'
     AND rejected_reason IS NOT NULL)
    OR
    (status IN ('draft', 'superseded'))
  )
);

CREATE INDEX engineering_change_orders_status_active
  ON engineering_change_orders(status)
  WHERE status IN ('draft', 'approved');

CREATE INDEX engineering_change_orders_effective
  ON engineering_change_orders(effective_at)
  WHERE status = 'approved';

COMMENT ON TABLE engineering_change_orders IS
  'Engineering Change Order workflow header. References bom_headers via '
  'bom_headers.eco_id (added in A4). post_eco_approve (B12 or '
  'acct-ir7) walks affected bom_headers and stamps effective_at on the '
  'new revisions; predecessor revisions get obsolete_at = ECO.effective_at, '
  'status=obsolete. acct-98e.';
