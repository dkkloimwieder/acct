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
