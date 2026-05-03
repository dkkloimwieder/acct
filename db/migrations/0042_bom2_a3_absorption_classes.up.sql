-- acct-iku / BOM2 Phase A3 — absorption_classes runtime taxonomy.
--
-- The fundamental design fix from Slice B: burden taxonomy lives in
-- runtime data, not the account_kind enum. Each row in this table
-- represents a distinct burden type the system can absorb against
-- WIP — labor, overhead, outside-processing, setup, tooling, energy,
-- whatever — with the absorption account_kind that the credit-side
-- of the apply event hits.
--
-- Adding a new burden type at runtime is now:
--   INSERT INTO absorption_classes (code, display_name, applied_account_kind)
--     VALUES ('osp_plating', 'Outside Plating', 'absorption_pool');
--   INSERT INTO accounts (kind, ledger_kind, currency, normal_side)
--     VALUES ('absorption_pool', 'value', 'USD', 'credit');
--   -- (B5 _wo_emit_bom_lines looks up the matching account by kind+currency)
--
-- Zero migration. Schema unchanged. The bom_lines table (A5) FKs
-- absorption_class_id to this table for service/charge lines.
--
-- expense_account_kind is nullable because actual-side wiring (payroll,
-- vendor bills for OSP) lands in Phase 2 (acct-9e2 PAYROLL,
-- acct-oef BURDENCLOSE). Class rows can be created with applied-only
-- now and have expense_account_kind filled in later when the closing
-- workflow needs it.
--
-- Seed: two canonical classes back-compat with existing tests.
-- labor_std maps to labor_applied (existing enum kind from Slice B).
-- oh_std maps to oh_applied. New burden types use absorption_pool
-- (added in A1) so adding more requires only data inserts.

CREATE TABLE absorption_classes (
  id                    BIGSERIAL PRIMARY KEY,
  code                  TEXT UNIQUE NOT NULL,
  display_name          TEXT NOT NULL,
  applied_account_kind  account_kind NOT NULL,
  expense_account_kind  account_kind,
  description           TEXT,
  is_active             BOOLEAN NOT NULL DEFAULT TRUE,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (expense_account_kind IS NULL OR applied_account_kind <> expense_account_kind)
);

CREATE INDEX absorption_classes_active_code
  ON absorption_classes(code) WHERE is_active;

COMMENT ON TABLE absorption_classes IS
  'Runtime-configurable burden taxonomy. Each row is one absorption '
  'kind (labor, OH, outside-processing, setup, tooling, ...). '
  'bom_lines (A5) FKs absorption_class_id to this table for '
  'service/charge lines. acct-iku.';

COMMENT ON COLUMN absorption_classes.applied_account_kind IS
  'account_kind for the credit-side of the apply event. Canonical '
  'kinds (labor_applied, oh_applied) for back-compat; '
  'absorption_pool (added A1) for runtime-added burden types.';

COMMENT ON COLUMN absorption_classes.expense_account_kind IS
  'Companion debit-normal kind for the actual-side cost (payroll, '
  'vendor bill, etc.). Nullable; Phase 2 acct-oef BURDENCLOSE close '
  'hook will net applied vs expense per class.';

INSERT INTO absorption_classes (code, display_name, applied_account_kind, expense_account_kind, description) VALUES
  ('labor_std', 'Standard Labor',    'labor_applied', 'labor_expense',
   'Direct labor absorption. Back-compat seed for Slice B; existing tests use labor_applied via this class.'),
  ('oh_std',    'Standard Overhead', 'oh_applied',    NULL,
   'Manufacturing overhead absorption. expense_account_kind nullable until Phase 2 actual-side wiring.');
