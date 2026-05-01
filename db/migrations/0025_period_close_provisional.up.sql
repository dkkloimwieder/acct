-- acct-4mt / acct-s6n.1 — Phase 1 period-close machinery: schema layer.
--
-- Foundational schema for the period-close orchestration that
-- acct-v51 (s6n.2) will build on. Two pieces:
--
--   1. transfers_provisional — side table marking transfers as having
--      a provisional cost component that must be finalized at period
--      close. The append-only trigger on transfers (migration 0008)
--      blocks UPDATE/DELETE on the transfer itself, so we can't add a
--      mutable finalized_at column to transfers directly. The side
--      table carries the lifecycle.
--
--   2. Three new account_kind enum values for the variance lines that
--      close-time finalization will post:
--        * variance_wac_period           — wac_periodic close adjustment
--        * variance_wac_retroactive      — wac_retroactive late-data correction
--        * variance_cost_adjust_retro    — cost_adjustment retroactive correction
--      Three separate kinds (not one shared 'variance_close') because
--      each accumulator tells a distinct story on the income
--      statement: a periodic-WAC variance is a method-of-costing
--      consequence, a retroactive-WAC variance is a late-data
--      correction, and a retroactive cost-adjustment is a
--      management-instructed revaluation. Keeping them separate
--      preserves audit clarity.
--
-- All three new kinds are bidirectional P&L (normal_side='unrestricted')
-- — write-ups credit them, write-downs debit them — same convention as
-- the existing variance_cost_adjustment and inv_adj_expense.
--
-- transfers_provisional state machine:
--
--                          INSERT (un-finalized)
--                                 │
--                                 ▼
--                  ┌──────────────────────────────┐
--                  │ finalized_at         = NULL  │
--                  │ variance_amount      = NULL  │
--                  │ variance_transfer_id = NULL  │
--                  └──────────────────────────────┘
--                                 │
--                       close_period() runs
--                                 │
--                                 ▼
--             ┌─────────────────────────────────────────┐
--             │ finalized_at         = clock_timestamp  │
--             │ variance_amount      = N (signed)       │
--             │ variance_transfer_id = NULL or transfer │
--             │  (NULL when variance_amount = 0,        │
--             │   non-NULL when variance_amount <> 0)   │
--             └─────────────────────────────────────────┘
--
-- The CHECK constraint enforces the three valid states (un-finalized,
-- finalized no-variance, finalized with variance) and rejects every
-- partial in-between.
--
-- Down migration drops the table; the new enum values stay (PG can't
-- cleanly drop enum values, and per project convention down-migrations
-- in Phase 0/1 leave additive enum changes in place).

ALTER TYPE account_kind ADD VALUE IF NOT EXISTS 'variance_wac_period';
ALTER TYPE account_kind ADD VALUE IF NOT EXISTS 'variance_wac_retroactive';
ALTER TYPE account_kind ADD VALUE IF NOT EXISTS 'variance_cost_adjust_retro';

CREATE TABLE transfers_provisional (
  transfer_id          BIGINT PRIMARY KEY REFERENCES transfers(id),
  period_id            BIGINT NOT NULL REFERENCES periods(id),
  cost_method          cost_method NOT NULL,
  finalized_at         TIMESTAMPTZ,
  variance_amount      BIGINT,
  variance_transfer_id BIGINT REFERENCES transfers(id),
  CHECK (
    CASE
      WHEN finalized_at IS NULL THEN
        variance_amount IS NULL AND variance_transfer_id IS NULL
      WHEN variance_amount = 0 THEN
        variance_transfer_id IS NULL
      ELSE
        variance_transfer_id IS NOT NULL
    END
  ),
  CHECK (variance_transfer_id IS NULL OR variance_transfer_id <> transfer_id)
);

-- Hot path for close_period(): "find every un-finalized row in this
-- period". Partial index keeps it tiny — only un-finalized rows.
CREATE INDEX tp_period_unfinalized
  ON transfers_provisional (period_id)
  WHERE finalized_at IS NULL;

-- Reporting / audit lookup: "what got finalized when".
CREATE INDEX tp_finalized_at
  ON transfers_provisional (finalized_at)
  WHERE finalized_at IS NOT NULL;
