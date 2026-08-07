-- accounting_period structural integrity (acct-1vur.3, design-v3.2 §7).
--
-- 0002 gave the table only UNIQUE (start_date, end_date), which permits two
-- shapes the close/admission machinery cannot survive:
--
--   1. INVERTED bounds (start_date > end_date). Every period predicate is
--      `posted_at >= start_date AND posted_at < end_date + 1`, so an inverted
--      row covers NOTHING: the 0017 guards silently stop protecting that
--      period, and a range constraint added later could not be built over it.
--
--   2. OVERLAPPING periods. Two periods covering the same date let one be
--      closed while the other stays open, so an event is simultaneously
--      inside a frozen period and legally appendable — and
--      `reject_out_of_order` (close.rs) reasons about a start_date ordering
--      that no longer implies a coverage ordering.
--
-- Both are structural, so both are constraints rather than API discipline —
-- there is no period-creation function to put the check in (periods are raw
-- INSERTs from the harness and the bench universe).
--
-- Deliberately NOT enforced: strict abutment (each period starting the day
-- after its predecessor ends). Gaps between periods are permitted. The
-- load-bearing pair against the wedge is overlap-exclusion here plus the
-- monotonic closed-frontier floor in 0022 — the floor rejects a backdate into
-- an uncovered gap on the strength of the closed frontier alone, so coverage
-- completeness is not required for correctness. Mandating abutment would be
-- calendar policy, and it would reject legitimate fixtures (a single-day test
-- period, a deliberately sparse bench universe) for no correctness gain.
--
-- The exclusion uses `daterange(start_date, end_date, '[]')` with the default
-- range_ops GiST opclass, so no btree_gist extension is required. '[]' matches
-- the inclusive end-date convention every predicate uses.

ALTER TABLE accounting_period
    ADD CONSTRAINT accounting_period_dates_ordered
    CHECK (start_date <= end_date);

ALTER TABLE accounting_period
    ADD CONSTRAINT accounting_period_no_overlap
    EXCLUDE USING gist (daterange(start_date, end_date, '[]') WITH &&);
