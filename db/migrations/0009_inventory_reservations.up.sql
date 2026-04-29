CREATE TABLE inventory_reservations (
  id          UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  sku_id      UUID NOT NULL REFERENCES skus(id),
  location_id UUID NOT NULL REFERENCES locations(id),
  qty         BIGINT NOT NULL CHECK (qty > 0),
  so_id       UUID NOT NULL REFERENCES sales_orders(id),
  so_line_id  UUID NOT NULL,
  status      reservation_status NOT NULL DEFAULT 'active',
  expires_at  TIMESTAMPTZ NOT NULL,
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  resolved_at TIMESTAMPTZ,
  unit_price  BIGINT,
  notes       TEXT
);

CREATE INDEX rsv_sku_loc_active
  ON inventory_reservations (sku_id, location_id)
  WHERE status = 'active';

CREATE INDEX rsv_so
  ON inventory_reservations (so_id);

CREATE INDEX rsv_expires
  ON inventory_reservations (expires_at)
  WHERE status = 'active';
