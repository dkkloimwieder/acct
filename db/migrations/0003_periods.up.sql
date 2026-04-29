CREATE TABLE periods (
  id        BIGSERIAL PRIMARY KEY,
  code      TEXT NOT NULL UNIQUE,
  opens_at  DATE NOT NULL,
  closes_at DATE NOT NULL,
  closed_at TIMESTAMPTZ,
  closed_by UUID,
  CHECK (opens_at <= closes_at)
);
