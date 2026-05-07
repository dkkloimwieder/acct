CREATE TABLE skus (
  id            UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  code          TEXT NOT NULL UNIQUE,
  description   TEXT,
  uom           TEXT NOT NULL,
  standard_cost BIGINT NOT NULL CHECK (standard_cost >= 0),
  cost_method   cost_method NOT NULL DEFAULT 'standard',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE locations (
  id         UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  code       TEXT NOT NULL UNIQUE,
  name       TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE sales_orders (
  id          UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  customer_id UUID,
  status      TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE purchase_orders (
  id          UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  supplier_id UUID,
  status      TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
