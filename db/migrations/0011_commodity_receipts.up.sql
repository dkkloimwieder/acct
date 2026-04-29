CREATE TABLE commodity_receipts (
  id                          UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  po_id                       UUID NOT NULL REFERENCES purchase_orders(id),
  po_line_id                  UUID NOT NULL,
  sku_id                      UUID NOT NULL REFERENCES skus(id),
  qty_received                BIGINT NOT NULL,
  provisional_price           BIGINT NOT NULL,
  final_price                 BIGINT,
  received_at                 TIMESTAMPTZ NOT NULL,
  settled_at                  TIMESTAMPTZ,
  settlement_formula          TEXT,
  qty_consumed_at_settlement  BIGINT,
  qty_on_hand_at_settlement   BIGINT
);
