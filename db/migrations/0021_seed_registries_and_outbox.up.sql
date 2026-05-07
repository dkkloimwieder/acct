-- Seed the close_hooks registry + create the optional async-outbox
-- staging table.
--
-- The cost_method_strategies registry is seeded inline at the bottom
-- of 0013 (where the per-strategy compute functions are defined).
-- The close_hooks registry is seeded here, last in the migration
-- chain, because the three hook functions (wac_periodic_close_hook,
-- wac_retroactive_close_hook, cost_adjust_retroactive_hook) live in
-- 0015 — registering them earlier wouldn't matter for correctness
-- (close_period EXECUTE-formats by name) but seeding last keeps the
-- registry contents in lockstep with the function bodies.
--
-- Ordering uses 10-unit gaps (10, 20, 30) so future hooks can
-- interleave at e.g. 15 or 25 without renumbering. The
-- cost_adjust_retroactive hook MUST run last (ordering=30): it layers
-- on top of WAC corrections by referencing the original depletion's
-- amount/qty, so WAC hooks must finalize before it runs.
--
-- ledger_outbox is the optional async-outbox staging table. Writers
-- can INSERT events here instead of calling post_posting_lines
-- synchronously; an external worker (or the in-process drain helper
-- used by load_outbox_* benches and outbox_drain) reads pending rows
-- in batches and replays them through post_posting_lines. Not on any
-- production hot path; the partial index keeps worker scans cheap.

INSERT INTO close_hooks (hook_fn_name, ordering, result_key, description)
VALUES
  ('wac_periodic_close_hook', 10, 'wac_periodic',
   'Recompute period-end avg per pool; finalize wac_periodic provisional rows.'),
  ('wac_retroactive_close_hook', 20, 'wac_retroactive',
   'Topological + chronological replay; finalize wac_retroactive provisional rows.'),
  ('cost_adjust_retroactive_hook', 30, 'cost_adjust_retroactive',
   'Flush queued retroactive cost-adjust rows; method-agnostic, runs after WAC hooks so it layers on top.');

CREATE TABLE ledger_outbox (
  id                     BIGSERIAL PRIMARY KEY,
  enqueued_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  events                 JSONB NOT NULL,
  override_closed_period BOOLEAN NOT NULL DEFAULT FALSE,
  status                 TEXT NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending','committed','failed')),
  committed_at           TIMESTAMPTZ,
  error_sqlstate         TEXT,
  error_text             TEXT,
  attempt_count          INT NOT NULL DEFAULT 0
);

CREATE INDEX ledger_outbox_pending
  ON ledger_outbox (id)
  WHERE status = 'pending';
