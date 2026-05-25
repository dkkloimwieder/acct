-- design-v3.1 §2.1 — enums.
-- cost_adjustment is intentionally NOT present in any enum: it would be added by
-- recalc/close (out of scope, §13) via a trivial ALTER TYPE ADD VALUE migration.

CREATE TYPE pool_method AS ENUM ('fifo', 'lifo', 'wac', 'std', 'specific');

CREATE TYPE pool_provisional_basis AS ENUM ('running_avg', 'standard');

CREATE TYPE trx_type AS ENUM (
    'po_receipt',
    'wo_completion',
    'inv_adjustment',
    'transfer_shipment',
    'transfer_receipt',
    'manual_adjustment',
    'revaluation_run'
);

CREATE TYPE line_type AS ENUM (
    'po_receipt_line',
    'wo_output',
    'wo_backflush',
    'wo_scrap',
    'inv_adjustment_line',
    'transfer_shipment_line',
    'transfer_receipt_line',
    'manual_adjustment_line',
    'revaluation_line'
);

CREATE TYPE posting_event_type AS ENUM (
    'inventory_receipt',
    'inventory_depletion',
    'wip_movement',
    'variance',
    'scrap',
    'adjustment',
    'revaluation'
);

CREATE TYPE account_type AS ENUM (
    'asset',
    'liability',
    'equity',
    'revenue',
    'expense'
);

CREATE TYPE dimension_type AS ENUM (
    'cost_center',
    'project',
    'department',
    'customer',
    'vendor'
);
