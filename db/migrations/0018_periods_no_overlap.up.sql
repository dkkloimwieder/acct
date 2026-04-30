-- Prevent overlapping period date ranges. Without this, post_transfers'
-- period lookup (SELECT INTO without STRICT) would silently pick an
-- arbitrary row when business_date falls in an overlap window.
--
-- Inclusive [opens_at, closes_at] daterange to match post_transfers'
-- inclusive lookup semantics. Adjacent periods (e.g. Apr 1..30 then
-- May 1..31) MUST remain legal — daterange's '[]' bounds plus &&
-- (overlaps, not touches) ensures that.
ALTER TABLE periods
  ADD CONSTRAINT periods_no_overlap
  EXCLUDE USING gist (
    daterange(opens_at, closes_at, '[]') WITH &&
  );
