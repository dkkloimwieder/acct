-- acct-b82 / Slice B.1 — Work-order schema (conversion cycle).
--
-- Adds the conversion-cycle reference + document tables. Function
-- bodies (post_wo_start, post_op_move, post_wo_complete, post_scrap)
-- land in migration 0038.
--
-- ============================================================
-- Design model: BOM + per-op burdens
-- ============================================================
--
-- A WO's standard cost rolls up as:
--
--   parent_std_cost = Σ (bom.qty_per_parent × component_std_cost)
--                   + Σ_op Σ_kind wo_routing_burdens.std_amount
--
-- Both BOM components and per-op burdens are per-unit costs that apply
-- to a unit as it moves through the routing. They differ in *what* they
-- accumulate (RM substance vs absorption / labor / OH / outside-proc /
-- tooling / setup / ...) and *when* they apply (RM at WO start; burdens
-- AT THE OP they belong to). Both concepts share the same per-unit /
-- per-op semantics.
--
-- ============================================================
-- Decisions locked at Slice B kickoff (2026-05-02)
-- ============================================================
--
-- Q1 — Routings: per-WO inline (`wo_routings`). No shared template
--      table for MVP; if reuse pressure becomes real later, a
--      `routing_templates` reference table can hang off without
--      reshaping the per-WO data.
-- Q2 — BOMs: formal `boms (parent, component, qty_per_parent,
--      component_loc_id)`. `post_wo_start` expands BOM rows to emit
--      one rm_issue_to_wo qty + value pair per component. P0029 if
--      parent has no BOM rows.
-- Q3 — Variance grain: residual at `wo_complete` only via `wo_close_v`
--      / `variance_wo_close`. Per-op MUV/LV/OHV deferred to Phase 2
--      reporting.
-- Q4 — Orphan WIP at period close: caller responsibility for MVP. No
--      fourth close hook. Phase 2 follow-up if user-experience pain
--      emerges.
-- Q5 — Burden timing: per-op (NOT all-at-WO-start). `post_wo_start`
--      applies first_op burdens only; subsequent ops' burdens apply at
--      `post_op_move` arrival into that op. Mirrors §3.4 semantics
--      ("labor applies when work begins on that op"). Each op_move
--      INTO an op (including rework moves) applies that op's burdens
--      against the moved qty.
-- Q6 — Burden taxonomy: open extension via `applied_account_kind`
--      column. MVP supports the existing absorption account_kinds
--      (`labor_applied`, `oh_applied`). New burden types
--      (outside_processing, setup, tooling, energy, etc.) require:
--        (a) `ALTER TYPE account_kind ADD VALUE '<X>_applied'`
--        (b) `ALTER TYPE transfer_reason ADD VALUE '<X>_apply'`
--        (c) `post_wo_start` / `post_op_move` CASE-mapping update
--        (d) per-currency `<X>_applied` account scaffolded
--      Schema does NOT change to accept a new burden type — the
--      `wo_routing_burdens` table is the open extension point.
--      Reconciliation between absorbed (X_applied credits) and actual
--      (vendor bill, payroll, allocation) is variance-account
--      territory, separate from this slice.
--
-- ============================================================
-- MVP cost-method restriction
-- ============================================================
--
-- Slice B MVP requires WO parent_sku.cost_method = 'standard'.
-- wac_perpetual / wac_periodic / wac_retroactive on a WO parent raise
-- P0006 at post_wo_start (Phase 2 acct-p7v lifts the restriction).
-- Component SKUs may use any cost method, but standard cost MUST be
-- in effect for them at p_business_date (`resolve_standard_cost_at`
-- raises P0018 otherwise) — the MVP values RM at standard regardless
-- of comp.cost_method to keep BOM expansion deterministic.
--
-- ============================================================
-- New transfer_reason: op_move_v
-- ============================================================
--
-- The dispatcher (_post_transfers_compute_amount) auto-prices cost-
-- relevant value events with reason ∈ {op_move, scrap, wo_complete,
-- so_ship}. Its standard branch returns
--   qty × resolve_standard_cost_at(parent_sku, business_date)
-- which is parent's FULL std cost. Correct for `wo_complete` (last op
-- accumulates full std), but WRONG for `op_move` between intermediate
-- ops because the WIP pool at op_from holds only RM + ops 1..from
-- burdens, not full parent_std. Fixing the dispatcher to read pool
-- accumulated unit cost on inv_value_wip class would extend wac-style
-- read-then-write to the standard path; for now we use a parallel
-- value-leg reason `op_move_v` (NOT in the dispatcher cost-event
-- list), letting `post_op_move` compute the amount externally and
-- pass it through. Mirrors the `scrap` / `scrap_v` qty-leg / value-leg
-- pattern.

-- ============================================================
-- Enum extension: op_move_v
-- ============================================================

ALTER TYPE transfer_reason ADD VALUE 'op_move_v';

-- ============================================================
-- work_orders
-- ============================================================

CREATE TABLE work_orders (
  id              UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  wo_no           TEXT NOT NULL UNIQUE,
  parent_sku_id   UUID NOT NULL REFERENCES skus(id),
  fg_location_id  UUID NOT NULL REFERENCES locations(id),
  qty_target      BIGINT NOT NULL CHECK (qty_target > 0),
  qty_completed   BIGINT NOT NULL DEFAULT 0 CHECK (qty_completed >= 0),
  qty_scrapped    BIGINT NOT NULL DEFAULT 0 CHECK (qty_scrapped >= 0),
  status          TEXT NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft', 'released', 'closed')),
  currency        CHAR(3) NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (qty_completed + qty_scrapped <= qty_target)
);

CREATE INDEX work_orders_parent_sku ON work_orders (parent_sku_id);
CREATE INDEX work_orders_status_open ON work_orders (status) WHERE status <> 'closed';

COMMENT ON TABLE work_orders IS
  'Work-order header. status: draft → released (after post_wo_start '
  'opens WIP@first_op) → closed (after post_wo_complete drains '
  'WIP@last_op and qty_completed + qty_scrapped reaches qty_target). '
  'currency is the WO''s value-side currency for inv_value_wip / '
  'labor_applied / oh_applied / variance_wo_close; multi-currency WOs '
  'must be split.';

-- ============================================================
-- wo_routings (per-WO inline operation list)
-- ============================================================

CREATE TABLE wo_routings (
  wo_id      UUID NOT NULL REFERENCES work_orders(id),
  routing_op INT  NOT NULL CHECK (routing_op > 0),
  op_name    TEXT NOT NULL,
  PRIMARY KEY (wo_id, routing_op)
);

COMMENT ON TABLE wo_routings IS
  'Per-WO routing operations. routing_op is the integer op number '
  '(10, 20, 30, ...) used as the inv_value_wip / stock_wip partition '
  'key (per migration 0020 unique index). Burdens for an op live in '
  'wo_routing_burdens — not as columns here — so adding new burden '
  'types is data, not schema.';

-- ============================================================
-- wo_routing_burdens (per-op, per-burden-kind absorption rates)
-- ============================================================

CREATE TABLE wo_routing_burdens (
  wo_id                UUID         NOT NULL,
  routing_op           INT          NOT NULL,
  applied_account_kind account_kind NOT NULL,
  std_amount           BIGINT       NOT NULL CHECK (std_amount >= 0),
  PRIMARY KEY (wo_id, routing_op, applied_account_kind),
  FOREIGN KEY (wo_id, routing_op) REFERENCES wo_routings(wo_id, routing_op)
);

COMMENT ON TABLE wo_routing_burdens IS
  'Per-op standard burden absorption rates. Each row is "apply '
  'std_amount per unit of <applied_account_kind> to WIP at routing_op". '
  'std_amount is per-unit-per-op (NOT per-WO-quantity). post_wo_start '
  'and post_op_move expand these rows into apply events:\n'
  '  WIP@op DR / <applied_account_kind>(currency) CR  (qty × std_amount)\n'
  'with reason derived from applied_account_kind: labor_applied → '
  'labor_apply, oh_applied → oh_apply. New burden types '
  '(outside_processing_applied, setup_applied, tooling_applied, ...) '
  'are added via ALTER TYPE account_kind ADD VALUE + ALTER TYPE '
  'transfer_reason ADD VALUE + post_wo_start CASE update — schema '
  'unchanged. The check that applied_account_kind is an absorption '
  'kind (kind ending in _applied, never a debit-normal kind like '
  'cogs) is runtime in the function; the FK is to wo_routings(wo_id, '
  'routing_op) so a row can''t reference an op the routing doesn''t '
  'declare.';

-- ============================================================
-- boms (parent → component reference data)
-- ============================================================

CREATE TABLE boms (
  parent_sku_id    UUID NOT NULL REFERENCES skus(id),
  component_sku_id UUID NOT NULL REFERENCES skus(id),
  component_loc_id UUID NOT NULL REFERENCES locations(id),
  qty_per_parent   BIGINT NOT NULL CHECK (qty_per_parent > 0),
  PRIMARY KEY (parent_sku_id, component_sku_id),
  CHECK (parent_sku_id <> component_sku_id)
);

CREATE INDEX boms_component_sku ON boms (component_sku_id);

COMMENT ON TABLE boms IS
  'Bill-of-materials reference data. parent_sku → component_sku at '
  'qty_per_parent units per WO unit. component_loc_id is the default '
  'issuing location for post_wo_start''s rm_issue_to_wo expansion. '
  'Phase 2 deferrals: revisions, effectivity windows, alternates, '
  'phantom assemblies. Single-level only — sub-assembly composition '
  'is a separate WO chained by a parent WO consuming a sub-assembly''s '
  'output as a comp.';
