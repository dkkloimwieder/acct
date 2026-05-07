-- acct-z49 / BOM2 Phase A4 — bom_headers table.
--
-- IMPORTANT REVISION FROM PLAN: A4 is additive only. The original plan
-- said "drop boms/wo_routing_burdens; create bom_headers" but dropping
-- the existing tables would break the live post_wo_start / post_op_move
-- functions which JOIN against them — and the 41 Slice B tests that
-- exercise those functions. Tests must stay green at every commit
-- boundary per the project's per-issue commit gating.
--
-- Strategy: create bom_headers (this migration) and bom_lines (A5)
-- alongside the existing boms + wo_routing_burdens. Existing functions
-- keep working against old tables. New functions in B1-B12 use the
-- new tables. C3 migrates wo_lifecycle.rs scaffolds to the new tables.
-- A FINAL cleanup migration (added to the plan as A4-cleanup, filed
-- post-execution) drops the old tables after all consumers migrated.
--
-- bom_headers represents "a way to build a parent SKU" — header for
-- alternates and revisions. work_orders.bom_id (A6) references this.
-- bom_lines (A5) FKs bom_id back to this table.
--
-- Alternates (alternate_no = 1, 2, 3, ...) are different ways to build
-- the same SKU at the same business_date — substitute material, alt
-- vendor, etc. One primary per (sku, alternate_no).
--
-- Revisions (revision_no = 'A', 'B', ...) are sequential evolutions
-- within an alternate. At any business_date, exactly one active revision
-- per (sku, alternate_no) — enforced by effective_at < obsolete_at +
-- the bom_header_at resolver function in B2.

CREATE TABLE bom_headers (
  id              BIGSERIAL PRIMARY KEY,
  parent_sku_id   UUID NOT NULL REFERENCES skus(id),
  alternate_no    INT  NOT NULL DEFAULT 1 CHECK (alternate_no >= 1),
  revision_no     TEXT NOT NULL DEFAULT 'A',
  code            TEXT,
  description     TEXT,
  status          TEXT NOT NULL DEFAULT 'active'
                  CHECK (status IN ('draft', 'active', 'obsolete')),
  is_primary      BOOLEAN NOT NULL DEFAULT FALSE,
  effective_at    TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
  obsolete_at     TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
  eco_id          BIGINT REFERENCES engineering_change_orders(id),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (parent_sku_id, alternate_no, revision_no),
  CHECK (effective_at < obsolete_at)
);

CREATE INDEX bom_headers_parent_alt
  ON bom_headers (parent_sku_id, alternate_no);

CREATE INDEX bom_headers_active
  ON bom_headers (parent_sku_id, alternate_no, effective_at, obsolete_at)
  WHERE status = 'active';

CREATE UNIQUE INDEX bom_headers_primary
  ON bom_headers (parent_sku_id, alternate_no)
  WHERE is_primary AND status = 'active';

CREATE INDEX bom_headers_eco
  ON bom_headers (eco_id) WHERE eco_id IS NOT NULL;

COMMENT ON TABLE bom_headers IS
  'BOM-as-a-thing. Each row is one way to build a parent SKU at a '
  'point in time. (parent_sku_id, alternate_no, revision_no) is unique. '
  'work_orders.bom_id (A6) references this. bom_lines (A5) holds the '
  'components/services/charges. acct-z49.';

COMMENT ON COLUMN bom_headers.alternate_no IS
  'Different ways to build the same SKU at the same business_date '
  '(substitute material, alt vendor, alt routing). One primary per '
  '(sku, alternate_no). Default 1.';

COMMENT ON COLUMN bom_headers.revision_no IS
  'Sequential evolution within an alternate (engineering changes). '
  'Effectivity windowed by effective_at / obsolete_at. Default ''A''.';

COMMENT ON INDEX bom_headers_primary IS
  'At most one primary bom_header per (parent_sku, alternate_no) at a '
  'given time. Used by _wo_resolve_bom_for (B3) when work_orders.bom_id '
  'is NULL — picks primary at business_date.';
