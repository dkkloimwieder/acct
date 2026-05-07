-- Periods: monthly accounting periods. Source-of-truth for the
-- period-lock invariant in post_posting_lines (consults
-- periods.closed_at — see 0014).

CREATE TABLE periods (
  id        BIGSERIAL PRIMARY KEY,
  code      TEXT NOT NULL UNIQUE,
  opens_at  DATE NOT NULL,
  closes_at DATE NOT NULL,
  closed_at TIMESTAMPTZ,
  closed_by UUID,
  CHECK (opens_at <= closes_at)
);

-- Prevent overlapping period date ranges. Without this,
-- post_posting_lines' period lookup (SELECT INTO without STRICT) would
-- silently pick an arbitrary row when business_date falls in an
-- overlap window. Inclusive [opens_at, closes_at] daterange matches
-- the inclusive lookup semantics; adjacent periods (e.g. Apr 1..30
-- then May 1..31) remain legal — daterange's '[]' bounds plus &&
-- (overlaps, not touches) ensures that.
ALTER TABLE periods
  ADD CONSTRAINT periods_no_overlap
  EXCLUDE USING gist (
    daterange(opens_at, closes_at, '[]') WITH &&
  );
