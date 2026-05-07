-- Seed the close_hooks registry.
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

INSERT INTO close_hooks (hook_fn_name, ordering, result_key, description)
VALUES
  ('wac_periodic_close_hook', 10, 'wac_periodic',
   'Recompute period-end avg per pool; finalize wac_periodic provisional rows.'),
  ('wac_retroactive_close_hook', 20, 'wac_retroactive',
   'Topological + chronological replay; finalize wac_retroactive provisional rows.'),
  ('cost_adjust_retroactive_hook', 30, 'cost_adjust_retroactive',
   'Flush queued retroactive cost-adjust rows; method-agnostic, runs after WAC hooks so it layers on top.');
